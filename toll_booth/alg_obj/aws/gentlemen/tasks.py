import json

import jsonref
from jsonschema import validate

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


class Versions(AlgObject):
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

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['workflow_versions'], json_dict['task_versions'])


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
            try:
                operation_arguments = operation_arguments.data_string
            except AttributeError:
                pass
            if operation_arguments is None:
                task_args[operation_name] = None
                continue
            for argument_name, arguments in operation_arguments.items():
                task_args[argument_name] = arguments
        return task_args

    @property
    def for_inspection(self):
        task_args = []
        for operation_name, operation_arguments in self._arguments.items():
            try:
                operation_arguments = operation_arguments.data_string
            except AttributeError:
                pass
            if operation_arguments is None:
                task_args[operation_name] = None
                continue
            for argument_name, arguments in operation_arguments.items():
                task_args.append([argument_name, arguments])
        return task_args

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['_arguments'])

    @classmethod
    def for_starting_data(cls, operation_name, starting_data):
        if isinstance(starting_data, TaskArguments):
            return starting_data
        stored_arguments = StoredData.from_object(operation_name, starting_data, full_unpack=False)
        return cls({operation_name: stored_arguments})

    @classmethod
    def from_schedule_event(cls, event: Event):
        event_input = json.loads(event.event_attributes['input'], cls=AlgDecoder)
        return cls(event_input)

    def add_argument_value(self, operation_name, arguments, identifier=None, overwrite=False):
        stored_name = operation_name
        if identifier:
            stored_name = f'{operation_name}{identifier}'
        stored_arguments = StoredData.from_object(stored_name, arguments, full_unpack=False)
        if operation_name in self._arguments and not overwrite:
            self._arguments[operation_name].merge(stored_arguments)
            return
        self._arguments[operation_name] = stored_arguments

    def add_argument_values(self, group_arguments, overwrite=False):
        for operation_name, arguments in group_arguments.items():
            self.add_argument_value(operation_name, arguments, overwrite=overwrite)

    def replace_argument_value(self, operation_name, arguments, identifier=None):
        persisted_task_args = {x: y for x, y in self._arguments.items() if x != operation_name}
        returned_args = TaskArguments(persisted_task_args)
        returned_args.add_argument_value(operation_name, arguments, identifier)
        return returned_args

    def get_argument_value(self, name):
        for operation_name, operation_arguments in self._arguments.items():
            argument_values = operation_arguments.data_string
            if argument_values is None:
                continue
            for argument_name, argument_value in argument_values.items():
                if argument_name == name:
                    return argument_value
        raise AttributeError(f'task arguments do not have argument value for key {name}')

    def merge_other_task_arguments(self, other, overwrite=False):
        if not isinstance(other, TaskArguments):
            raise NotImplementedError
        self.add_argument_values(other.arguments, overwrite=overwrite)

    def __getitem__(self, item):
        return self._arguments[item]

    def get(self, item, default=None):
        try:
            return self.__getitem__(item)
        except KeyError:
            return default


class LeechConfigEntry(AlgObject):
    def __init__(self, config_items=None):
        if not config_items:
            config_items = {}
        self._config_items = config_items

    @classmethod
    def parse_json(cls, json_dict):
        return json_dict['config_items']

    def add_config_item(self, config_name, config_value):
        self._config_items[config_name] = config_value

    @property
    def config_items(self):
        return self._config_items

    def __iter__(self):
        return iter(self._config_items)

    def __getitem__(self, item):
        return self._config_items[item]

    def get(self, item, default=None):
        try:
            return self._config_items[item]
        except KeyError:
            return default


class LeechConfig(AlgObject):
    def __init__(self, workflow_configs=None, task_configs=None):
        if not workflow_configs:
            workflow_configs = {}
        if not task_configs:
            task_configs = {}
        self._workflow_configs = workflow_configs
        self._task_configs = task_configs

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['workflow_configs'], json_dict['task_configs'])

    @classmethod
    def retrieve(cls, **kwargs):
        from toll_booth.alg_obj.aws.snakes.schema_snek import SchemaSnek
        config_file_name = kwargs.get('CONFIG_FILE', 'config.json')
        snek = SchemaSnek(**kwargs)
        current_config = snek.get_schema(config_file_name)
        return cls._build_config(current_config)

    @classmethod
    def post(cls, config_file_path, validation_file_path, **kwargs):
        from toll_booth.alg_obj.aws.snakes.schema_snek import SchemaSnek
        config_file_name = kwargs.get('CONFIG_FILE', 'config.json')
        validation_file_name = kwargs.get('MASTER_CONFIG_FILE', 'master_config.json')
        snek = SchemaSnek(**kwargs)
        with open(config_file_path) as config_file, open(validation_file_path) as validation_file:
            working_config = jsonref.load(config_file)
            master_config = jsonref.load(validation_file)
            validate(working_config, master_config)
            snek.put_schema(config_file_path, config_file_name)
            snek.put_schema(validation_file_path, validation_file_name)
            return cls._build_config(working_config)

    @classmethod
    def _build_config(cls, json_dict):
        cls_kwargs = {'workflow_configs': {}, 'task_configs': {}}
        config_body = json_dict['domain']
        for entry in config_body['workflows']:
            config_entry = LeechConfigEntry()
            workflow_name = entry['workflow_name']
            for config_name, config_item in entry['workflow_config'].items():
                config_entry.add_config_item(config_name, config_item)
            cls_kwargs['workflow_configs'][workflow_name] = config_entry
        for entry in config_body['tasks']:
            config_entry = LeechConfigEntry()
            task_name = entry['task_name']
            for config_name, config_item in entry['task_config'].items():
                config_entry.add_config_item(config_name, config_item)
            cls_kwargs['task_configs'][task_name] = config_entry
        return cls(**cls_kwargs)

    @property
    def workflow_configs(self):
        return self._workflow_configs

    @property
    def task_configs(self):
        return self._task_configs

    def __getitem__(self, item):
        item_type = item[0]
        item_name = item[1]
        if item_type == 'task':
            return self._task_configs[item_name]
        if item_type == 'workflow':
            return self._workflow_configs[item_name]
        raise KeyError(item)

    def get_task_config(self, task_name):
        return self._task_configs[task_name]

    def get_workflow_config(self, workflow_name):
        return self._workflow_configs[workflow_name]
