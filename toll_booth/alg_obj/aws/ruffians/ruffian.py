import json
import logging
import os
import threading
from multiprocessing.pool import ThreadPool
from queue import Queue
from threading import Thread

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from retrying import retry

from toll_booth.alg_obj.serializers import AlgEncoder


class RuffianRoost:
    @classmethod
    def conscript_ruffians(cls, decider_list, work_lists, domain_name, config, **kwargs):
        default_machine_arn = os.getenv('RUFFIAN_MACHINE', 'arn:aws:states:us-east-1:803040539655:stateMachine:ruffians')
        default_deciding_machine_arn = os.getenv('DECIDING_MACHINE', 'arn:aws:states:us-east-1:803040539655:stateMachine:decider')
        ruffian_machine_arn = kwargs.get('machine_arn', default_machine_arn)
        deciding_machine_arn = kwargs.get('deciding_machine_arn', default_deciding_machine_arn)
        run_config = kwargs.get('run_config', {})
        client = boto3.client('stepfunctions')
        execution_arns = [cls._start_machine(deciding_machine_arn, decider_list, domain_name, config, run_config, client)]
        running_machines = []
        for work_list in work_lists:
            execution_arns.append(
                cls._start_machine(ruffian_machine_arn, work_list, domain_name, config, run_config, client)
            )
            running_machines.append(work_list['list_name'])
        if decider_list not in running_machines:
            base_ruffian = {'list_name': decider_list, 'number_threads': 1}
            execution_arns.append(cls._start_machine(ruffian_machine_arn, base_ruffian, domain_name, config, run_config, client))
        return execution_arns

    @classmethod
    def _build_machine_name(cls, work_list):
        import uuid

        banned_characters = [
            '?', '*', '<', '>', '{', '}', '[', ']',
            ':', ';', ',', '\\', '|', '^', '~', '$',
            '#', '%', '&', '`', '"', ' '
        ]
        abbreviations = {
            'PROGRAM': 'PGM',
            'EMPLOYEE': 'EMP',
            'ASSIGNMENT': 'ASGMT',
            'ACCEPT': 'ACPT',
            'REQUEST': 'RQST',
            'PRESCRIPTION': 'RX',
            'SUCCESS': 'SCS',
            'FAILURE': 'FALR',
            'CONFIRM': 'CNFRM',
            'REVOCATION': 'RVCTN',
            'EXOSTAR': 'XSTR',
            'DELETED': 'DLTD',
            'DELETE': 'DLT',
            'DIAGNOSIS': 'DX',
            'SUPPORT': 'SUPT',
            'REMOVED': 'RMVD',
            'SUBSCRIPTION': 'SBSCPTN',
            'PROOFING': 'PRFNG',
            'PROFILE': 'PRFL'
        }
        machine_id = work_list
        if 'list_name' in work_list:
            machine_id = work_list['list_name']
        for original_word, replacement_word in abbreviations.items():
            machine_id = machine_id.replace(original_word, replacement_word)
        for entry in banned_characters:
            machine_id = machine_id.replace(entry, '')
        if len(machine_id) > 70:
            machine_id = hash(machine_id)
        machine_name = f'{machine_id}!!{uuid.uuid4().hex}'
        machine_name = machine_name[:80]
        return machine_name

    @classmethod
    def _start_machine(cls, machine_arn, work_list, domain_name, config, run_config, client=None):
        if not client:
            client = boto3.client('stepfunctions')
        machine_input = json.dumps({
            'work_list': work_list,
            'domain_name': domain_name,
            'config': config,
            'run_config': run_config
        }, cls=AlgEncoder)
        machine_name = cls._build_machine_name(work_list)
        response = client.start_execution(
            stateMachineArn=machine_arn,
            name=machine_name,
            input=machine_input
        )
        execution_arn = response['executionArn']
        return execution_arn

    @classmethod
    def disband_ruffians(cls, execution_arn):
        client = boto3.client('stepfunctions')
        try:
            client.stop_execution(
                executionArn=execution_arn,
                cause='work completed'
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'ExecutionDoesNotExist':
                raise e


class Ruffian:
    def __init__(self, domain_name, work_list, config, warn_level, context, **kwargs):
        run_config = kwargs.get('run_config', {})
        self._domain_name = domain_name
        self._work_list = work_list
        self._config = config
        self._warn_level = warn_level
        self._context = context
        self._pending_tasks = {}
        self._connections = []
        self._run_config = run_config

    @classmethod
    def build(cls, context, domain_name, work_list, config, **kwargs):
        warn_seconds = kwargs.get('warn_seconds', 120)
        run_config = kwargs.get('run_config', {})
        warn_level = (warn_seconds * 1000)
        return cls(domain_name, work_list, config, warn_level, context, run_config=run_config)

    def _check_watch(self):
        time_remaining = self._context.get_remaining_time_in_millis()
        logging.debug(f'ruffian checked their watch, remaining time in millis: {time_remaining}')
        return time_remaining

    def supervise(self):
        from toll_booth.alg_obj.aws.gentlemen.command import General

        logging.info(f'starting up a ruffian as a supervisor for task_list: {self._work_list}, for domain_name: {self._domain_name}, with warn_level: {self._warn_level}, with run_config: {self._run_config}')
        time_remaining = self._check_watch()
        while time_remaining >= self._warn_level:
            general = General(self._domain_name, self._work_list, self._context, run_config=self._run_config)
            try:
                general.command()
            except Exception as e:
                import traceback
                trace = traceback.format_exc()
                logging.error(f'error occurred in the decide task for list {self._work_list}: {e}, trace: {trace}')
            time_remaining = self._check_watch()

    def labor(self):
        work_list_name = self._work_list['list_name']
        logging.info(f'starting up a ruffian as a worker for task_list: {work_list_name}, for domain_name: {self._domain_name}, with warn_level: {self._warn_level}')
        threads = []
        queue = Queue()
        labor_args = {
            'list_name': work_list_name,
            'queue': queue
        }
        labor_components = [self._dispatch_tasks]
        for component in labor_components:
            component_thread = Thread(target=component, kwargs=labor_args)
            component_thread.start()
            threads.append(component_thread)
        time_remaining = self._check_watch()
        while time_remaining >= self._warn_level:
            if len(self._pending_tasks) <= self._work_list['number_threads']:
                poll_results = self._poll_for_tasks()
                if 'taskToken' in poll_results:
                    queue.put({'task_type': 'new_task', 'poll_response': poll_results})
            time_remaining = self._check_watch()
        logging.info(f'time is up, preparing to quit')
        queue.put(None)
        for thread in threads:
            thread.join()

    def _poll_for_tasks(self):
        from toll_booth.alg_obj.aws.gentlemen.labor import Laborer
        domain_name = self._domain_name
        list_name = self._work_list['list_name']
        laborer = Laborer(domain_name, list_name)
        poll_response = laborer.poll_for_tasks()
        logging.info(f'received a response from polling {domain_name} for task_list: {list_name}, {poll_response}')
        return poll_response

    def _dispatch_tasks(self, **kwargs):
        queue = kwargs['queue']
        pool = ThreadPool()
        while True:
            new_task = queue.get()
            if new_task is None:
                return
            task_type = new_task['task_type']
            if task_type == 'new_task':
                logging.info(f'received orders to dispatch a task: {new_task}')
                poll_response = new_task['poll_response']
                task_token = poll_response['taskToken']
                task_args = {
                    'queue': queue,
                    'poll_response': poll_response
                }
                pending = pool.apply_async(self._run_task, kwds=task_args)
                self._pending_tasks[task_token] = pending
                logging.info(f'started the task in a thread, total pending tasks: {len(self._pending_tasks)}')
            if task_type == 'close_task':
                logging.info(f'received orders to close a task thread: {new_task}')
                task_token = new_task['task_token']
                # self._pending_tasks[task_token].join()
                del(self._pending_tasks[task_token])
                logging.info(f'closed out a task thread, total pending tasks: {len(self._pending_tasks)}')

    def _run_task(self, **kwargs):
        logging.info(f'task thread started: {kwargs}')
        swf_client = boto3.client('swf')
        task_token = kwargs['poll_response']['taskToken']
        swf_payload = {'taskToken': task_token}
        try:
            results = self._fire_task(**kwargs)
        except Exception as e:
            swf_payload.update({'reason': e.args[0], 'details': ','.join(e.args)})
            swf_client.respond_activity_task_failed(**swf_payload)
            return
        try:
            if results['fail'] is True:
                swf_payload.update({'reason': results['reason'], 'details': results['details']})
                swf_client.respond_activity_task_failed(**swf_payload)
        except TypeError:
            swf_payload.update({'result': json.dumps(results, cls=AlgEncoder)})
            try:
                swf_client.respond_activity_task_completed(**swf_payload)
            except ClientError as e:
                logging.error(e.response)
        kwargs['queue'].put({'task_type': 'close_task', 'task_token': task_token})

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=10000)
    def _fire_task(self, **kwargs):
        from toll_booth.alg_obj.serializers import AlgDecoder

        client = boto3.client('lambda', config=Config(connect_timeout=600, read_timeout=600))
        task_list = self._work_list['list_name']
        poll_response = kwargs['poll_response']
        task_name = poll_response['activityType']['name']
        task_args = poll_response['input']
        lambda_fn_name = os.getenv('LABOR_FUNCTION', 'leech-lambda-labor')
        config = self._config[('task', task_name)]
        is_vpc = config.get('is_vpc', False)
        if is_vpc:
            lambda_fn_name = os.getenv('VPC_LABOR_FUNCTION', 'leech-vpc-labor')
        logging.info(f'firing a controlled activity for {task_list}, named {task_name} with args: {task_args}')
        response = client.invoke(
            FunctionName=lambda_fn_name,
            InvocationType='RequestResponse',
            Payload=task_args
        )
        raw_result = response['Payload'].read().decode()
        try:
            results = json.loads(json.loads(raw_result), cls=AlgDecoder)
        except TypeError as e:
            results = {
                'fail': True
            }
            if 'errorMessage' in raw_result:
                results['reason'] = 'lambda_error'
                results['details'] = raw_result
                return results
            logging.error(e.args[0])
            raise RuntimeError()
        logging.info(f'got the results from controlled activity {task_name} back: {results}')
        return results
