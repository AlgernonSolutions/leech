import json
import logging
import os
import time
from multiprocessing import Pipe, Process

import boto3
from botocore.exceptions import ReadTimeoutError

from toll_booth.alg_obj.aws.gentlemen.tasks import Task


class RuffianName:
    def __init__(self, flow_id, run_id):
        self._flow_id = flow_id
        self._run_id = run_id

    @property
    def name(self):
        return f'{self._flow_id}-{self._run_id}'

    def __str__(self):
        return self.name


class RuffianRoost:
    @classmethod
    def conscript_ruffians(cls, decider_list, work_lists, domain_name, is_vpc=False, **kwargs):
        default_machine_arn = os.getenv('RUFFIAN_MACHINE', 'arn:aws:states:us-east-1:803040539655:stateMachine:ruffians')
        if is_vpc:
            default_machine_arn = os.getenv('VPC_RUFFIAN_MACHINE', '')
        machine_arn = kwargs.get('machine_arn', default_machine_arn)
        client = boto3.client('stepfunctions')
        machine_input = json.dumps({
            'decider_list': decider_list,
            'work_lists': work_lists,
            'domain_name': domain_name
        })
        response = client.start_execution(
            stateMachineArn=machine_arn,
            input=machine_input
        )
        execution_arn = response['executionArn']
        return execution_arn

    @classmethod
    def disband_ruffians(cls, execution_arn):
        client = boto3.client('stepfunctions')
        client.stop_execution(
            executionArn=execution_arn
        )


class Ruffian:
    def __init__(self, domain_name, work_lists, warn_level, context):
        self._domain_name = domain_name
        self._work_lists = work_lists
        self._warn_level = warn_level
        self._context = context
        self._rackets = []
        self._connections = []

    @classmethod
    def build(cls, context, domain_name, work_lists, **kwargs):
        warn_seconds = kwargs.get('warn_seconds', 45)
        warn_level = (warn_seconds * 1000)
        return cls(domain_name, work_lists, warn_level, context)

    def _check_watch(self):
        return self._context.get_remaining_time_in_millis()

    def supervise(self):
        from toll_booth.alg_obj.aws.gentlemen.command import General

        while True:
            time_remaining = self._check_watch()
            if time_remaining <= self._warn_level:
                return
            general = General(self._domain_name, self._work_lists)
            try:
                general.command()
            except ReadTimeoutError:
                continue
            except Exception as e:
                import traceback
                trace = traceback.format_exc()
                logging.error(f'error occurred in the decide task for list {self._work_lists}: {e}, trace: {trace}')

    def labor(self):
        for task_list_name, num_workers in self._work_lists.items():
            parent_connection, child_connection = Pipe()
            self._connections.append(parent_connection)
            process_args = (child_connection, self._domain_name, task_list_name, num_workers)
            task_list_process = Process(target=self._work_task_list, args=process_args)
            task_list_process.start()
            self._rackets.append(task_list_process)
        time_remaining = self._check_watch()
        while time_remaining >= self._warn_level:
            time.sleep(15)
            time_remaining = self._check_watch()
        for connection in self._connections:
            connection.send(None)
        for racket in self._rackets:
            racket.join()

    @staticmethod
    def _check_orders(connection, wait_time=1):
        has_message = connection.poll(wait_time)
        if has_message:
            orders = connection.recv()
            if orders is None:
                connection.close()
                return True
        return False

    @staticmethod
    def _notify_task(task_payload):
        client = boto3.client('swf')
        success = task_payload[0]
        if not success:
            return client.respond_activity_task_failed(
                taskToken=task_payload[1],
                reason=task_payload[2],
                details=task_payload[3]
            )
        client.respond_activity_task_completed(
            taskToken=task_payload[1],
            result=task_payload[2]
        )

    def _manage_pending_tasks(self, pending_tasks):
        client = boto3.client('swf')
        outstanding_tasks = []
        for pending_task in pending_tasks:
            task_connection = pending_task['connection']
            has_results = task_connection.poll(1)
            if has_results is False:
                outstanding_tasks.append(pending_task)
                client.record_activity_task_heartbeat(
                    taskToken=pending_task['token']
                )
                continue
            self._notify_task(task_connection.recv())
            task_connection.close()
            pending_task['process'].join()
        return outstanding_tasks

    def _work_task_list(self, connection, domain_name, task_list, num_workers):
        from toll_booth.alg_obj.aws.gentlemen.labor import Laborer

        logging.info(f'starting a worker to work task_list: {task_list}')
        pending_tasks = []
        while True:
            laborer = Laborer(domain_name, task_list)
            if self._check_orders(connection):
                return
            pending_tasks = self._manage_pending_tasks(pending_tasks)
            if len(pending_tasks) > num_workers:
                logging.info(f'number of tasks exceeds allotted concurrency for {task_list}, waiting')
                continue
            try:
                poll_response = laborer.poll_for_tasks()
                logging.info(f'received a response from polling for task_list: {task_list}, {poll_response}')
                parent_connection, child_connection = Pipe()
                labor_process = Process(target=self._labor, args=(child_connection, poll_response))
                labor_process.start()
                pending_tasks.append({
                    'connection': parent_connection,
                    'process': labor_process,
                    'token': poll_response['taskToken']
                })
            except ReadTimeoutError:
                continue
            except Exception as e:
                import traceback
                trace = traceback.format_exc()
                logging.error(f'error occurred in the labor task for list {task_list}: {e}, trace: {trace}')

    def _labor(self, connection, poll_response):
        import json
        from toll_booth.alg_obj.serializers import AlgEncoder

        task = Task.parse_from_poll(poll_response)
        logging.info(f'received a task from the queue: {task.activity_name}')
        try:
            results = self._work_task(task)
            result_string = json.dumps(results, cls=AlgEncoder)
            return_payload = (True, task.task_token, result_string)
        except Exception as e:
            import traceback
            failure_reason = json.dumps(e.args)
            trace = traceback.format_exc()
            failure_details = json.dumps({
                'task_name': task.activity_name,
                'task_version': task.activity_version,
                'trace': trace
            })
            return_payload = (False, task.task_token, failure_reason, failure_details)
        logging.info(f'done with task: {task.activity_name}, return payload: {return_payload}')
        connection.send(return_payload)
        connection.close()

    def _work_task(self, task: Task):
        from toll_booth.alg_tasks.rivers.tasks.fungi import fungi_tasks

        task_modules = [fungi_tasks]
        task_name = task.activity_name
        for task_module in task_modules:
            task_fn = getattr(task_module, task_name, None)
            if task_fn:
                results = task_fn(**task.task_args.for_task)
                return results
        raise NotImplementedError('could not find a registered task for type: %s' % task_name)
