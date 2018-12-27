_lambda_steps = {
    'operation_first': 'LambdaFunctionScheduled',
    'execution_first': 'LambdaFunctionStarted',
    'all': [
        'LambdaFunctionScheduled',
        'LambdaFunctionStarted',
        'LambdaFunctionCompleted',
        'LambdaFunctionFailed',
        'LambdaFunctionTimedOut',
        'ScheduleLambdaFunctionFailed'
        'StartLambdaFunctionFailed',
    ],
    'live': ['LambdaFunctionScheduled', 'LambdaFunctionStarted'],
    'failed': [
        'LambdaFunctionFailed', 'LambdaFunctionTimedOut',
        'ScheduleLambdaFunctionFailed', 'StartLambdaFunctionFailed'
    ],
    'completed': 'LambdaFunctionCompleted'
}
_subtask_steps = {
    'operation_first': 'StartChildWorkflowExecutionInitiated',
    'execution_first': 'ChildWorkflowExecutionStarted',
    'all': [
        'StartChildWorkflowExecutionInitiated',
        'StartChildWorkflowExecutionFailed',
        'ChildWorkflowExecutionStarted',
        'ChildWorkflowExecutionCompleted',
        'ChildWorkflowExecutionFailed',
        'ChildWorkflowExecutionTimedOut',
        'ChildWorkflowExecutionCanceled',
        'ChildWorkflowExecutionTerminated',
    ],
    'live': [
        'ChildWorkflowExecutionStarted',
        'StartChildWorkflowExecutionInitiated'
    ],
    'failed': [
        'StartChildWorkflowExecutionFailed',
        'ChildWorkflowExecutionTimedOut',
        'ChildWorkflowExecutionCanceled',
        'ChildWorkflowExecutionTerminated'
    ],
    'completed': 'ChildWorkflowExecutionCompleted'

}

_starting_step = 'WorkflowExecutionStarted'


class Event:
    def __init__(self, event_id, event_type, event_timestamp, event_attributes):
        self._event_id = event_id
        self._event_type = event_type
        self._event_timestamp = event_timestamp
        self._event_attributes = event_attributes

    @classmethod
    def parse_from_decision_poll_event(cls, poll_response_event):
        event_attributes = {}
        for key, value in poll_response_event.items():
            if 'EventAttributes' in key:
                event_attributes = value
        return cls(
            poll_response_event['eventId'],
            poll_response_event['eventType'],
            poll_response_event['eventTimestamp'],
            event_attributes
        )

    def __str__(self):
        return str(self._event_type)

    def __getattr__(self, item):
        return self._event_attributes[item]

    @property
    def event_type(self):
        return self._event_type

    @property
    def event_id(self):
        return self._event_id

    @property
    def event_timestamp(self):
        return self._event_timestamp

    @property
    def event_attributes(self):
        return self._event_attributes


'LambdaFunctionScheduled'
'LambdaFunctionStarted'
'LambdaFunctionCompleted'
'LambdaFunctionFailed'
'LambdaFunctionTimedOut'
'StartLambdaFunctionFailed'
'ScheduleLambdaFunctionFailed'


class LambdaExecution:
    def __init__(self, run_id: str, events: [Event] = None):
        if not events:
            events = []
        self._run_id = run_id
        self._events = events

    @property
    def run_id(self):
        return self._run_id

    @property
    def status(self):
        time_sorted = sorted(self._events, key=lambda x: x.event_timestamp, reverse=True)
        return str(time_sorted[0])

    @property
    def is_live(self):
        return self.status in _lambda_steps['live']

    @property
    def is_failed(self):
        return self.status in _lambda_steps['failed']

    @property
    def is_completed(self):
        return self.status == _lambda_steps['completed']

    @property
    def fail_reason(self):
        if self.status in _lambda_steps['failed']:
            return self.status
        return None

    @property
    def results(self):
        time_sorted = sorted(self._events, key=lambda x: x.event_timestamp, reverse=True)
        return time_sorted[0].event_attributes['result']

    def add_event(self, event: Event):
        self._events.append(event)


class LambdaOperation:
    def __init__(self, fn_name: str, flow_id: str, run_id: str, task_args: str, lambda_executions: [LambdaExecution] = None):
        if not lambda_executions:
            lambda_executions = []
        self._fn_name = fn_name
        self._flow_id = flow_id
        self._run_id = run_id
        self._task_args = task_args
        self._lambda_executions = lambda_executions
        self._operation_failure = None

    @property
    def run_id(self):
        return self._run_id

    @property
    def results(self):
        returned_results = []
        for execution in self._lambda_executions:
            if execution.results:
                returned_results.append(execution.results)
        if len(returned_results) > 1:
            raise RuntimeError('multiple lambda invocations of the same workflow with the same input '
                               'must return the same values, this was not the case')
        for result in returned_results:
            return result
        return None

    @property
    def is_live(self):
        for execution in self._lambda_executions:
            if execution.is_live:
                return True
        return False

    @property
    def is_complete(self):
        for execution in self._lambda_executions:
            if execution.is_completed:
                return True
        return False

    def add_execution(self, lambda_execution: LambdaExecution):
        self._lambda_executions.append(lambda_execution)

    def set_operation_failure(self, event: Event):
        self._operation_failure = event

    def invoke(self):
        return


class SubtaskExecution:
    def __init__(self, flow_id: str, run_id: str, task_name: str, events: [Event] = None):
        if not events:
            events = []
        self._flow_id = flow_id
        self._run_id = run_id
        self._task_name = task_name
        self._events = events

    @property
    def flow_id(self):
        return self._flow_id

    @property
    def run_id(self):
        return self._run_id

    @property
    def status(self):
        time_sorted = sorted(self._events, key=lambda x: x.event_timestamp, reverse=True)
        return str(time_sorted[0])

    @property
    def is_live(self):
        return self.status in _subtask_steps['live']

    @property
    def is_failed(self):
        return self.status in _subtask_steps['failed']

    @property
    def is_completed(self):
        return self.status == _subtask_steps['completed']

    @property
    def fail_reason(self):
        if self.status in _subtask_steps['failed']:
            return self.status
        return None

    @property
    def results(self):
        time_sorted = sorted(self._events, key=lambda x: x.event_timestamp, reverse=True)
        if time_sorted:
            try:
                return time_sorted[0].event_attributes['result']
            except KeyError:
                return None
        return None

    def add_event(self, event: Event):
        self._events.append(event)


class SubtaskOperation:
    def __init__(self, flow_id: str, task_name: str, task_args: str, subtask_executions: [SubtaskExecution] = None):
        if not subtask_executions:
            subtask_executions = []
        self._flow_id = flow_id
        self._task_name = task_name
        self._task_args = task_args
        self._subtask_executions = subtask_executions
        self._operation_failure = None

    @property
    def flow_id(self):
        return self._flow_id

    @property
    def task_name(self):
        return self._task_name

    @property
    def results(self):
        returned_results = []
        for execution in self._subtask_executions:
            if execution.results:
                returned_results.append(execution.results)
        if len(returned_results) > 1:
            raise RuntimeError('multiple lambda invocations of the same workflow with the same input '
                               'must return the same values, this was not the case')
        for result in returned_results:
            return result
        return None

    @property
    def is_live(self):
        for execution in self._subtask_executions:
            if execution.is_live:
                return True
        return False

    @property
    def is_complete(self):
        for execution in self._subtask_executions:
            if execution.is_completed:
                return True
        return False

    def add_execution(self, subtask_execution: SubtaskExecution):
        self._subtask_executions.append(subtask_execution)

    def set_operation_failure(self, event: Event):
        self._operation_failure = event


class WorkflowHistory:
    def __init__(self, flow_type: str, task_token: str, flow_id: str, run_id: str, lambda_role: str, input_str: str,
                 events: [Event], subtask_operations: [SubtaskOperation], lambda_operations: [LambdaOperation]):
        self._flow_type = flow_type
        self._flow_id = flow_id
        self._run_id = run_id
        self._lambda_role = lambda_role
        self._input_str = input_str
        self._task_token = task_token
        self._events = events
        self._lambda_operations = lambda_operations
        self._subtask_operations = subtask_operations

    @classmethod
    def parse_from_poll(cls, poll_response):
        flow_type = poll_response['workflowType']['name']
        task_token = poll_response['taskToken']
        execution_info = poll_response['workflowExecution']
        flow_id = execution_info['workflowId']
        run_id = execution_info['runId']
        raw_events = poll_response['events']
        events = [Event.parse_from_decision_poll_event(x) for x in raw_events]
        lambda_operations = cls._generate_lambda_operations(events)
        subtask_operations = cls._generate_subtask_operations(events)
        input_str, lambda_role = cls._generate_workflow_starter_data(events)
        cls_args = {
            'flow_type': flow_type, 'task_token': task_token, 'flow_id': flow_id, 'run_id': run_id,
            'input_str': input_str, 'lambda_role': lambda_role, 'events': events,
            'subtask_operations': subtask_operations, 'lambda_operations': lambda_operations
        }
        return cls(**cls_args)

    @classmethod
    def _generate_lambda_operations(cls, events: [Event]):
        lambda_operations = {}
        lambda_executions = {}
        lambda_events = []
        for event in events:
            if event.event_type in _lambda_steps['all']:
                if event.event_type == _lambda_steps['operation_first']:
                    fn_name = event.event_attributes.get('name')
                    flow_id = event.event_attributes['id']
                    run_id = event.event_id
                    task_args = event.event_attributes['input']
                    lambda_operation = LambdaOperation(fn_name, flow_id, run_id, task_args)
                    lambda_operations[fn_name] = lambda_operation
                    continue
                if event.event_type == _lambda_steps['execution_first']:
                    operation_event_id = event.event_attributes['scheduledEventId']
                    lambda_execution = LambdaExecution(operation_event_id, [event])
                    lambda_executions[operation_event_id] = lambda_execution
                    continue
                lambda_events.append(event)
        for event in lambda_events:
            if event.event_type == 'ScheduleLambdaFunctionFailed':
                fn_name = event.event_attributes['name']
                lambda_operations[fn_name].set_operation_failure(event)
                continue
            execution_id = event.event_attributes['scheduledEventId']
            lambda_executions[execution_id].add_event(event)
        for execution_id, lambda_execution in lambda_executions.items():
            matched = False
            for lambda_operation in lambda_operations.values():
                if execution_id == lambda_operation.run_id:
                    lambda_operation.add_execution(lambda_execution)
                    matched = True
                    continue
            if not matched:
                raise RuntimeError('could not find appropriate lambda operation for '
                                   'lambda execution: %s' % lambda_execution)
        return lambda_operations

    @classmethod
    def _generate_subtask_operations(cls, events: [Event]):
        subtask_operations = {}
        subtask_executions = {}
        subtask_events = {}
        for event in events:
            if event.event_type in _subtask_steps['all']:
                try:
                    flow_id = event.event_attributes['workflowId']
                except KeyError:
                    flow_id = event.event_attributes['workflowExecution']['workflowId']
                task_name = event.event_attributes['workflowType']['name']
                if flow_id not in subtask_events:
                    subtask_events[flow_id] = []
                if event.event_type == _subtask_steps['operation_first']:
                    task_args = event.event_attributes['input']
                    subtask_operation = SubtaskOperation(flow_id, task_name, task_args)
                    subtask_operations[flow_id] = subtask_operation
                    continue
                if event.event_type == _subtask_steps['execution_first']:
                    run_id = event.event_attributes['workflowExecution']['runId']
                    subtask_execution = SubtaskExecution(flow_id, run_id, task_name, [event])
                    subtask_executions[run_id] = subtask_execution
                    continue
                subtask_events[flow_id].append(event)
        for flow_id, event_set in subtask_events.items():
            for event in event_set:
                if event.event_type == 'StartChildWorkflowExecutionFailed':
                    subtask_operations[flow_id].set_operation_failure(event)
                    continue
                run_id = event.event_attributes['workflowExecution']['runId']
                subtask_executions[run_id].add_event(event)
        for run_id, subtask_execution in subtask_executions.items():
            matched = False
            execution_flow_id = subtask_execution.flow_id
            for subtask_operation in subtask_operations.values():
                if execution_flow_id == subtask_operation.flow_id:
                    subtask_operation.add_execution(subtask_execution)
                    matched = True
                    continue
            if not matched:
                raise RuntimeError('could not find appropriate subtask operation for '
                                   'subtask execution: %s' % subtask_execution)
        return subtask_operations

    @classmethod
    def _generate_workflow_starter_data(cls, events: [Event]):
        for event in events:
            if event.event_type == _starting_step:
                return event.event_attributes['input'], event.event_attributes['lambdaRole']

    @property
    def flow_type(self):
        return self._flow_type

    @property
    def flow_id(self):
        return self._flow_id

    @property
    def task_token(self):
        return self._task_token

    @property
    def flow_input(self):
        return self._input_str

    @property
    def lambda_role(self):
        return self._lambda_role

    def get_lambda_operation(self, fn_name):
        try:
            return self._lambda_operations[fn_name]
        except KeyError:
            return None

    def get_subtask_operation(self, task_name):
        try:
            return self._subtask_operations[task_name]
        except KeyError:
            return None

    def add_events(self, events: [Event]):
        self._events.add_events(events)


'WorkflowExecutionStarted'
'WorkflowExecutionCancelRequested'
'WorkflowExecutionCompleted'
'CompleteWorkflowExecutionFailed'
'WorkflowExecutionFailed'
'FailWorkflowExecutionFailed'
'WorkflowExecutionTimedOut'
'WorkflowExecutionCanceled'
'CancelWorkflowExecutionFailed'
'WorkflowExecutionContinuedAsNew'
'ContinueAsNewWorkflowExecutionFailed'
'WorkflowExecutionTerminated'
'DecisionTaskScheduled'
'DecisionTaskStarted'
'DecisionTaskCompleted'
'DecisionTaskTimedOut'
'ActivityTaskScheduled'
'ScheduleActivityTaskFailed'
'ActivityTaskStarted'
'ActivityTaskCompleted'
'ActivityTaskFailed'
'ActivityTaskTimedOut'
'ActivityTaskCanceled'
'ActivityTaskCancelRequested'
'RequestCancelActivityTaskFailed'
'WorkflowExecutionSignaled'
'MarkerRecorded'
'RecordMarkerFailed'
'TimerStarted'
'StartTimerFailed'
'TimerFired'
'TimerCanceled'
'CancelTimerFailed'
'StartChildWorkflowExecutionInitiated'
'StartChildWorkflowExecutionFailed'
'ChildWorkflowExecutionStarted'
'ChildWorkflowExecutionCompleted'
'ChildWorkflowExecutionFailed'
'ChildWorkflowExecutionTimedOut'
'ChildWorkflowExecutionCanceled'
'ChildWorkflowExecutionTerminated'
'SignalExternalWorkflowExecutionInitiated'
'SignalExternalWorkflowExecutionFailed'
'ExternalWorkflowExecutionSignaled'
'RequestCancelExternalWorkflowExecutionInitiated'
'RequestCancelExternalWorkflowExecutionFailed'
'ExternalWorkflowExecutionCancelRequested'
'LambdaFunctionScheduled'
'LambdaFunctionStarted'
'LambdaFunctionCompleted'
'LambdaFunctionFailed'
'LambdaFunctionTimedOut'
'ScheduleLambdaFunctionFailed'
'StartLambdaFunctionFailed'
