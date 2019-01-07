import json

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.aws.snakes.snakes import StoredData
from toll_booth.alg_obj.serializers import AlgDecoder


class Task:
    def __init__(self, task_token, activity_id, flow_id, run_id, activity_name, activity_version, task_args):
        self._task_token = task_token
        self._activity_id = activity_id
        self._flow_id = flow_id
        self._run_id = run_id
        self._activity_name = activity_name
        self._activity_version = activity_version
        self._task_args = task_args

    @property
    def task_token(self):
        return self._task_token

    @property
    def activity_name(self):
        return self._activity_name

    @property
    def activity_version(self):
        return self._activity_version

    @property
    def flow_id(self):
        return self._flow_id

    @property
    def run_id(self):
        return self._run_id

    @property
    def task_args(self):
        return self._task_args

    @classmethod
    def parse_from_poll(cls, poll_response):
        task_token = poll_response['taskToken']
        activity_id = poll_response['activityId']
        flow_execution = poll_response['workflowExecution']
        flow_id = flow_execution['workflowId']
        run_id = flow_execution['runId']
        activity_data = poll_response['activityType']
        activity_name = activity_data['name']
        activity_version = activity_data['version']
        task_args = json.loads(poll_response['input'], cls=AlgDecoder)
        return cls(task_token, activity_id, flow_id, run_id, activity_name, activity_version, task_args)


class OperationName:
    def __init__(self, fn_name, execution_id, specifiers=None):
        if not specifiers:
            specifiers = []
        self._fn_name = fn_name
        self._execution_id = execution_id
        self._specifiers = specifiers

    @property
    def name(self):
        return f'{self._fn_name}{self.specifiers}-{self._execution_id}'

    @property
    def base_stem(self):
        return f'{self._fn_name}{self.specifiers}'

    @property
    def specifiers(self):
        if not self._specifiers:
            return
        content = '-'.join(self._specifiers)
        return f'{"-"}{content}'

    def __str__(self):
        return self.name


class Versions:
    def __init__(self, workflow_versions, task_versions):
        self._workflow_versions = workflow_versions
        self._task_versions = task_versions

    @property
    def workflow_versions(self):
        return self._workflow_versions

    @property
    def task_versions(self):
        return self._task_versions

    @classmethod
    def retrieve(cls, domain_name='Leech'):
        from multiprocessing.dummy import Pool as ThreadPool
        pool = ThreadPool(2)
        work = [(domain_name, False), (domain_name, True)]
        results = pool.map(cls._retrieve, work)
        pool.close()
        pool.join()
        return cls(*results)

    @classmethod
    def _retrieve(cls, *args):
        import boto3
        args = args[0]
        domain_name = args[0]
        retrieve_activities = args[1]
        paginator_operation = 'list_workflow_types'
        type_name = 'workflowType'
        if retrieve_activities:
            paginator_operation = 'list_activity_types'
            type_name = 'activityType'
        results = {}
        client = boto3.client('swf')
        paginator = client.get_paginator(paginator_operation)
        response_iterator = paginator.paginate(
            domain=domain_name,
            registrationStatus='REGISTERED',
            reverseOrder=True
        )
        for page in response_iterator:
            for entry in page['typeInfos']:
                activity_info = entry[type_name]
                activity_name = activity_info['name']
                version = int(activity_info['version'])
                if activity_name not in results:
                    results[activity_name] = version
                    continue
                if version > results[activity_name]:
                    results[activity_name] = version
        return results


class TaskArguments(AlgObject):
    def __init__(self, arguments: {str: {str: StoredData}}):
        self._arguments = arguments

    @property
    def arguments(self):
        return self._arguments

    @property
    def for_task(self):
        task_args = {}
        for operation_name, operation_arguments in self._arguments.items():
            if operation_arguments is None:
                task_args[operation_name] = None
                continue
            operation_task_args = {}
            try:
                operation_arguments = operation_arguments.data_string
            except AttributeError:
                pass
            for argument_name, arguments in operation_arguments.items():
                operation_task_args[argument_name] = arguments
            task_args[operation_name] = operation_task_args
        return task_args

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['_arguments'])

    @classmethod
    def for_starting_data(cls, starting_data):
        stored_arguments = StoredData.from_object('start', starting_data, full_unpack=False)
        return cls({'start': stored_arguments})

    @classmethod
    def from_schedule_event(cls, event: Event):
        event_input = json.loads(event.event_attributes['input'], cls=AlgDecoder)
        return cls(event_input)

    def add_arguments(self, arguments):
        for operation_name, operation_args in arguments.items():
            self._arguments[operation_name] = operation_args

    def add_argument_value(self, operation_name, arguments):
        stored_arguments = StoredData.from_object(operation_name, arguments, full_unpack=False)
        self._arguments[operation_name] = stored_arguments
