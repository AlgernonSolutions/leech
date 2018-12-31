class Task:
    def __init__(self, task_token, activity_id, flow_id, run_id, activity_name, activity_version, input_string):
        self._task_token = task_token
        self._activity_id = activity_id
        self._flow_id = flow_id
        self._run_id = run_id
        self._activity_name = activity_name
        self._activity_version = activity_version
        self._input_string = input_string

    @property
    def task_token(self):
        return self._task_token

    @property
    def activity_name(self):
        return self._activity_name

    @property
    def input_string(self):
        return self._input_string

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
        input_string = poll_response['input']
        return cls(task_token, activity_id, flow_id, run_id, activity_name, activity_version, input_string)


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
