import json
import logging
import os
from multiprocessing.pool import ThreadPool
from queue import Queue
from threading import Thread

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from retrying import retry

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder


class RuffianId(AlgObject):
    def __init__(self, domain_name, flow_id, flow_name, list_name, **kwargs):
        is_laborer = kwargs.get('is_laborer', False)
        self._domain_name = domain_name
        self._flow_id = flow_id
        self._flow_name = flow_name
        self._list_name = list_name
        self._is_laborer = is_laborer

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['domain_name'], json_dict['flow_id'], json_dict['flow_name'],
            json_dict['list_name'], is_laborer=json_dict.get('is_laborer')
        )

    @classmethod
    def from_raw(cls, raw_string, flow_id=None):
        raw_parts = raw_string.split('#')
        is_laborer = raw_parts[3] == 'True'
        return cls(raw_parts[0], flow_id, raw_parts[1], raw_parts[2], is_laborer=is_laborer)

    @classmethod
    def for_overseer(cls, domain_name):
        flow = 'ruffianing'
        return cls(domain_name, flow, flow, flow)

    @property
    def domain_name(self):
        return self._domain_name

    @property
    def flow_id(self):
        return self._flow_id

    @property
    def flow_name(self):
        return self._flow_name

    @property
    def list_name(self):
        return self._list_name

    @property
    def is_laborer(self):
        return self._is_laborer

    def as_overseer_item(self, execution_arn, start_time):
        item = self.as_overseer_key
        item.update({
            'start_time': start_time,
            'running': True,
            'list_name': self._list_name,
            'is_laborer': self._is_laborer,
            'flow_name': self._flow_name,
            'execution_arn': execution_arn,
            'domain_name': self._domain_name
        })
        return item

    @property
    def as_overseer_key(self):
        if self._flow_id is None:
            raise RuntimeError('attempted to submit a ruffian_id to the db, but the flow_id is not set')
        return {'workflow_id': self._flow_id, 'ruffian_id': str(self)}

    def __str__(self):
        return f'{self._domain_name}#{self._flow_name}#{self._list_name}#{self._is_laborer}'


class RuffianRoost:
    _default_machine_arn = os.getenv('RUFFIAN_MACHINE', 'arn:aws:states:us-east-1:803040539655:stateMachine:ruffians')
    _default_deciding_machine_arn = os.getenv('DECIDING_MACHINE',
                                              'arn:aws:states:us-east-1:803040539655:stateMachine:decider')

    @classmethod
    def conscript_ruffian(cls, ruffian_id: RuffianId, config=None, flow_id=None, flow_name=None, **kwargs):
        ruffian_machine_arn = kwargs.get('machine_arn', cls._default_machine_arn)
        machine_arn = kwargs.get('deciding_machine_arn', cls._default_deciding_machine_arn)
        task_list = ruffian_id.list_name
        run_config = kwargs.get('run_config', {})
        if not flow_name:
            flow_name = ruffian_id.flow_name
        if not flow_id:
            flow_id = ruffian_id.flow_id
        if ruffian_id.is_laborer:
            machine_arn = ruffian_machine_arn
        return cls._start_machine(machine_arn, task_list, ruffian_id.domain_name, config, run_config, flow_id=flow_id, flow_name=flow_name, **kwargs)

    @classmethod
    def conscript_ruffians(cls, decider_list, work_lists, domain_name, config, **kwargs):
        ruffian_machine_arn = kwargs.get('machine_arn', cls._default_machine_arn)
        deciding_machine_arn = kwargs.get('deciding_machine_arn', cls._default_deciding_machine_arn)
        run_config = kwargs.get('run_config', {})
        client = boto3.client('stepfunctions')
        decider_machine_name = cls._build_machine_name(decider_list)
        decider_kwargs = {'machine_name': decider_machine_name, 'client': client}
        execution_arns = [
            cls._start_machine(deciding_machine_arn, decider_list, domain_name, config, run_config, **decider_kwargs)]
        running_machines = []
        for work_list in work_lists:
            machine_name = cls._build_machine_name(work_list['list_name'])
            machine_kwargs = {'machine_name': machine_name, 'client': client}
            execution_arns.append(
                cls._start_machine(ruffian_machine_arn, work_list, domain_name, config, run_config, **machine_kwargs)
            )
            running_machines.append(work_list['list_name'])
        if decider_list not in running_machines:
            base_ruffian = {'list_name': decider_list, 'number_threads': 1}
            execution_arns.append(cls._start_machine(ruffian_machine_arn, base_ruffian, domain_name, config, run_config,
                                                     **decider_kwargs))
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
    def _start_machine(cls, machine_arn, work_list, domain_name, config, run_config, **kwargs):
        client = kwargs.get('client')
        if not client:
            client = boto3.client('stepfunctions')
        machine_name = kwargs.get('machine_name')
        is_overseer = kwargs.get('is_overseer')
        input_values = {
            'work_list': work_list,
            'domain_name': domain_name,
            'config': config,
            'run_config': run_config,
            'flow_id': kwargs.get('flow_id'),
            'flow_name': kwargs.get('flow_name'),
            'overseer_token': kwargs.get('overseer_token'),
            'ruffian_config': kwargs.get('ruffian_config')
        }
        if is_overseer:
            input_values['is_overseer'] = True
        machine_input = json.dumps(input_values, cls=AlgEncoder)
        start_kwargs = {
            'stateMachineArn': machine_arn,
            'input': machine_input
        }
        if machine_name:
            start_kwargs['name'] = machine_name
        response = client.start_execution(**start_kwargs)
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

    @classmethod
    def generate_ruffians(cls, domain_name, flow_id, flow_name, leech_config, run_config):
        decider_ruffian = {
            'ruffian_id': RuffianId(domain_name, flow_id, flow_name, flow_id, is_laborer=False),
            'run_config': run_config
        }
        base_task_ruffian = {
            'ruffian_id': RuffianId(domain_name, flow_id, flow_name, flow_id, is_laborer=True),
            'ruffian_config': {'task_list': flow_id, 'number_threads': 1}
        }
        ruffians = [decider_ruffian, base_task_ruffian]
        ruffian_ids = [x['ruffian_id'] for x in ruffians]
        workflow_config = leech_config.get_workflow_config(flow_name)
        labor_task_lists = workflow_config.get('labor_task_lists', [])
        for entry in labor_task_lists:
            entry_ruffian_id = RuffianId(domain_name, flow_id, flow_name, entry['list_name'], is_laborer=True)
            if entry_ruffian_id not in ruffian_ids:
                ruffians.append({
                    'ruffian_id': entry_ruffian_id,
                    'ruffian_config': entry
                })
        return ruffians


class Ruffian:
    def __init__(self, domain_name, flow_name, work_list, config, warn_level, context, **kwargs):
        run_config = kwargs.get('run_config', {})
        ruffian_config = kwargs.get('ruffian_config', {})
        self._overseer_token = kwargs.get('overseer_token')
        self._domain_name = domain_name
        self._flow_name = flow_name
        self._work_list = work_list
        self._config = config
        self._warn_level = warn_level
        self._context = context
        self._pending_tasks = {}
        self._connections = []
        self._run_config = run_config
        self._ruffian_config = ruffian_config

    @classmethod
    def build(cls, context, domain_name, flow_name, work_list, config, **kwargs):
        warn_seconds = kwargs.get('warn_seconds', 120)
        run_config = kwargs.get('run_config', {})
        ruffian_config = kwargs.get('ruffian_config', {})
        overseer_token = kwargs.get('overseer_token')
        warn_level = (warn_seconds * 1000)
        return cls(domain_name, flow_name, work_list, config, warn_level, context,
                   run_config=run_config, ruffian_config=ruffian_config, overseer_token=overseer_token)

    def _check_watch(self):
        time_remaining = self._context.get_remaining_time_in_millis()
        logging.debug(f'ruffian checked their watch, remaining time in millis: {time_remaining}')
        return time_remaining

    def _send_ndy(self):
        client = boto3.client('swf')
        response = client.record_activity_task_heartbeat(
            taskToken=self._overseer_token
        )
        return response['cancelRequested'] is False

    def oversee(self):
        from toll_booth.alg_obj.aws.gentlemen.command import General

        logging.info(
            f'starting up a ruffian as a overseer for task_list: {self._work_list}, for domain_name: {self._domain_name}, with warn_level: {self._warn_level}, with run_config: {self._run_config}')
        time_remaining = self._check_watch()
        self._run_config['fresh_start'] = True
        while time_remaining >= (self._warn_level * 1.5):
            general = General(self._domain_name, self._work_list, self._context, run_config=self._run_config)
            try:
                command_results = general.command()
                if command_results:
                    for _ in range(command_results):
                        poll_response = self._poll_for_tasks(self._work_list)
                        task_token = poll_response['taskToken']
                        input_values = json.loads(poll_response['input'], cls=AlgDecoder)
                        arg_values = input_values['task_args'].for_task
                        arg_values['overseer_token'] = task_token
                        if 'config' not in arg_values:
                            arg_values['config'] = self._config
                        self._manage_ruffians(**arg_values)
            except Exception as e:
                import traceback
                trace = traceback.format_exc()
                logging.error(f'error occurred in the decide task for list {self._work_list}: {e}, trace: {trace}')
            time_remaining = self._check_watch()

    @classmethod
    def _manage_ruffians(cls, **kwargs):
        ruffian_action = kwargs['signal_name']
        if ruffian_action == 'start_ruffian':
            return cls._rouse_ruffian(**kwargs)
        if ruffian_action == 'stop_ruffian':
            return cls._disband_ruffian(**kwargs)
        raise NotImplementedError(f'can not perform ruffian action: {ruffian_action}')

    @classmethod
    def _rouse_ruffian(cls, **kwargs):
        ruffian_id = RuffianId.from_raw(kwargs['ruffian_id'])
        kwargs['ruffian_config'] = kwargs.get('ruffian_config')
        kwargs['ruffian_id'] = ruffian_id
        execution_arn = RuffianRoost.conscript_ruffian(**kwargs)
        return {str(ruffian_id): execution_arn}

    @classmethod
    def _disband_ruffian(cls, overseer_token):
        client = boto3.client('swf')
        client.respond_activity_task_completed(
            taskToken=overseer_token
        )

    def supervise(self):
        from toll_booth.alg_obj.aws.gentlemen.command import General

        logging.info(
            f'starting up a ruffian as a supervisor for task_list: {self._work_list}, for domain_name: {self._domain_name}, with warn_level: {self._warn_level}, with run_config: {self._run_config}')
        time_remaining = self._check_watch()
        keep_working = True
        while time_remaining >= self._warn_level and keep_working:
            general = General(self._domain_name, self._work_list, self._context, run_config=self._run_config)
            try:
                general.command()
            except Exception as e:
                import traceback
                trace = traceback.format_exc()
                logging.error(f'error occurred in the decide task for list {self._work_list}: {e}, trace: {trace}')
            time_remaining = self._check_watch()
            keep_working = self._send_ndy()
        return {'keep_working': keep_working}

    def labor(self):
        logging.info(
            f'starting up a ruffian as a worker for task_list: {self._work_list}, for domain_name: {self._domain_name}, with warn_level: {self._warn_level}')
        threads = []
        queue = Queue()
        labor_args = {
            'list_name': self._work_list,
            'queue': queue
        }
        labor_components = [self._dispatch_tasks]
        for component in labor_components:
            component_thread = Thread(target=component, kwargs=labor_args)
            component_thread.start()
            threads.append(component_thread)
        time_remaining = self._check_watch()
        keep_working = True
        while time_remaining >= self._warn_level and keep_working:
            if len(self._pending_tasks) <= self._ruffian_config['number_threads']:
                poll_results = self._poll_for_tasks()
                if 'taskToken' in poll_results:
                    queue.put({'task_type': 'new_task', 'poll_response': poll_results})
            time_remaining = self._check_watch()
            keep_working = self._send_ndy()
        logging.info(f'time is up, preparing to quit')
        queue.put(None)
        for thread in threads:
            thread.join()
        self._send_ndy()
        return {'keep_working': keep_working}

    def _poll_for_tasks(self, list_name=None):
        from toll_booth.alg_obj.aws.gentlemen.labor import Laborer
        domain_name = self._domain_name
        if not list_name:
            list_name = self._work_list
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
                del (self._pending_tasks[task_token])
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
        task_list = self._work_list
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
