import json
import logging
import os
from queue import Queue
from threading import Thread

import boto3
from botocore.exceptions import ReadTimeoutError


class RuffianRoost:
    @classmethod
    def conscript_ruffians(cls, decider_list, work_lists, domain_name, **kwargs):
        default_machine_arn = os.getenv('RUFFIAN_MACHINE', 'arn:aws:states:us-east-1:803040539655:stateMachine:ruffians')
        default_vpc_machine_arn = os.getenv('VPC_RUFFIAN_MACHINE', 'arn:aws:states:us-east-1:803040539655:stateMachine:vpc_ruffians')
        default_deciding_machine_arn = os.getenv('DECIDING_MACHINE', 'arn:aws:states:us-east-1:803040539655:stateMachine:decider')
        ruffian_machine_arn = kwargs.get('machine_arn', default_machine_arn)
        vpc_ruffian_machine_arn = kwargs.get('vpc_machine_arn', default_vpc_machine_arn)
        deciding_machine_arn = kwargs.get('deciding_machine_arn', default_deciding_machine_arn)
        client = boto3.client('stepfunctions')
        execution_arns = [cls._start_machine(deciding_machine_arn, decider_list, domain_name, client)]
        for work_list in work_lists:
            list_machine_arn = ruffian_machine_arn
            if work_lists.get('is_vpc', False) is True:
                list_machine_arn = vpc_ruffian_machine_arn
            execution_arns.append(
                cls._start_machine(list_machine_arn, work_list, domain_name, client)
            )
        return execution_arns

    @classmethod
    def _start_machine(cls, machine_arn, work_list, domain_name, client=None):
        if not client:
            client = boto3.client('stepfunctions')
        machine_input = json.dumps({
            'work_list': work_list,
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
        work_list_name = self._work_lists['list_name']
        logging.info(f'starting a worker to work task_list: {work_list_name}')
        threads = []
        queue = Queue()
        labor_args = {
            'list_name': work_list_name,
            'number_threads': self._work_lists['number_threads'],
            'queue': queue
        }
        labor_components = [self._dispatch_tasks]
        for component in labor_components:
            component_thread = Thread(target=component, kwargs=labor_args)
            component_thread.start()
            threads.append(component_thread)
        time_remaining = self._check_watch()
        while time_remaining >= self._warn_level:
            poll_results = self._poll_for_tasks()
            queue.put({'task_type': 'new_task', 'poll_response': poll_results})
            time_remaining = self._check_watch()
        queue.put(None)
        for thread in threads:
            thread.join()

    def _poll_for_tasks(self):
        from toll_booth.alg_obj.aws.gentlemen.labor import Laborer
        domain_name = self._domain_name
        list_name = self._work_lists['list_name']
        laborer = Laborer(domain_name, list_name)
        poll_response = laborer.poll_for_tasks()
        logging.info(f'received a response from polling {domain_name} for task_list: {list_name}, {poll_response}')
        return poll_response

    def _dispatch_tasks(self, **kwargs):
        queue = kwargs['queue']
        number_threads = kwargs['number_threads']
        pending_tasks = {}
        while True:
            new_task = queue.get()
            if new_task is None:
                return
            task_type = new_task['task_type']
            if task_type == 'new_task':
                if len(pending_tasks) > number_threads:
                    queue.put(new_task)
                    continue
                poll_response = new_task['poll_response']
                task_token = poll_response['taskToken']
                task_args = {
                    'queue': queue,
                    'poll_response': poll_response
                }
                pending = Thread(target=self._run_task, kwargs=task_args)
                pending.start()
                pending_tasks[task_token] = pending
            if task_type == 'close_task':
                task_token = new_task['task_token']
                pending_tasks[task_token].join()
                del(pending_tasks[task_token])

    def _run_task(self, **kwargs):
        swf_client = boto3.client('swf')
        task_token = kwargs['poll_response']['taskToken']
        results = self._fire_task(**kwargs)
        swf_payload = {'taskToken': task_token}
        if results['fail'] is True:
            swf_payload.update({'reason': results['reason'], 'details': results['details']})
            swf_client.respond_activity_task_failed(**swf_payload)
        else:
            swf_payload.update({'result': results['result']})
            swf_client.respond_activity_task_completed(**swf_payload)
        kwargs['queue'].put({'task_type': 'close_task', 'task_token': task_token})

    def _fire_task(self, **kwargs):
        from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder

        client = boto3.client('lambda')
        task_list = self._work_lists['list_name']
        poll_response = kwargs['poll_response']
        task_name = poll_response['activityType']['name']
        task_args = poll_response['input']
        lambda_payload = {'task_name': task_name, 'task_args': task_args, 'register_results': True}
        logging.info(f'firing a controlled activity for {task_list}, named {task_name} with args: {task_args}')
        response = client.invoke(
            FunctionName=os.getenv('LABOR_FUNCTION', 'leech-lambda-labor'),
            InvocationType='RequestResponse',
            Payload=json.dumps(lambda_payload, cls=AlgEncoder)
        )
        raw_result = response['Payload'].read().decode()
        results = json.loads(json.loads(raw_result), cls=AlgDecoder)
        logging.info(f'got the results from controlled activity {task_name} back: {results}')
        return results
