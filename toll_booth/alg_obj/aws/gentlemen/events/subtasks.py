from toll_booth.alg_obj.aws.gentlemen.events.base_history import History, Operation, Execution
from toll_booth.alg_obj.aws.gentlemen.events.events import Event

_steps = {
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
    'failure': 'StartChildWorkflowExecutionFailed',
    'completed': 'ChildWorkflowExecutionCompleted'

}


class SubtaskExecution(Execution):
    def __init__(self, flow_id: str, run_id: str, task_name: str, events: [Event]):
        self._flow_id = flow_id
        self._run_id = run_id
        self._task_name = task_name
        self._events = events

    @classmethod
    def generate_from_start_event(cls, event: Event):
        cls_args = {
            'flow_id': event.event_attributes['workflowExecution']['workflowId'],
            'run_id': event.event_attributes['workflowExecution']['runId'],
            'task_name': event.event_attributes['workflowType']['name'],
            'events': [event]
        }
        return cls(**cls_args)

    @property
    def flow_id(self):
        return self._flow_id


class SubtaskOperation(Operation):
    def __init__(self, flow_id: str, task_name: str, task_version: str, task_args: str, lambda_role: str, task_list_name: str,  subtask_executions: [SubtaskExecution]):
        self._flow_id = flow_id
        self._name = task_name
        self._task_version = task_version
        self._task_args = task_args
        self._executions = subtask_executions
        self._lambda_role = lambda_role
        self._task_list_name = task_list_name
        self._operation_failure = None

    @classmethod
    def generate_from_schedule_event(cls, event: Event):
        cls_args = {
            'flow_id': event.event_attributes['workflowId'],
            'task_name': event.event_attributes['workflowType']['name'],
            'task_version': event.event_attributes['workflowType']['version'],
            'task_args': event.event_attributes['input'],
            'lambda_role': event.event_attributes['lambdaRole'],
            'task_list_name': event.event_attributes['taskList']['name'],
            'subtask_executions': [event]
        }
        return cls(**cls_args)

    @property
    def task_name(self):
        return self._name

    @property
    def task_version(self):
        return self._task_version

    @property
    def task_args(self):
        return self._task_args

    @property
    def lambda_role(self):
        return self._lambda_role

    @property
    def task_list_name(self):
        return self._task_list_name

    @property
    def subtask_executions(self):
        return self._executions


class SubtaskHistory(History):
    def __init__(self, operations: [SubtaskOperation] = None):
        if not operations:
            operations = []
        self._operations = operations

    def get_operation_failed_count(self, flow_id, fail_reason=None):
        failed_count = 0
        operations = self.get_by_id(flow_id)
        for operation in operations:
            for execution in operation.executions:
                if execution.is_failed:
                    if fail_reason:
                        if execution.fail_reason != fail_reason:
                            continue
                        failed_count += 1
        return failed_count

    def _add_operation_event(self, event: Event):
        if event.event_attributes['id'] in self.flow_ids:
            return
        subtask_operation = SubtaskOperation.generate_from_schedule_event(event)
        self._operations.append(subtask_operation)
        return

    def _add_execution_event(self, event: Event):
        run_id = event.event_attributes['scheduledEventId']
        subtask_execution = SubtaskExecution.generate_from_start_event(event)
        for operation in self._operations:
            if run_id == operation.run_id:
                if event.event_id in operation.event_ids:
                    return
                operation.add_execution(subtask_execution)
                return
            raise RuntimeError('could not find appropriate subtask operation for '
                               'subtask execution: %s' % subtask_execution)

    def _add_failure_event(self, event: Event):
        flow_id = event.event_attributes['workflowExecution']['workflowId']
        for operation in self._operations:
            if operation.flow_id == flow_id:
                operation.set_operation_failure(event)
                return
        raise RuntimeError('attempted to add a failure event to a non-existent subtask operation')

    def _add_general_event(self, event: Event):
        run_id = event.event_attributes['scheduledEventId']
        for operation in self._operations:
            for execution in operation.lambda_executions:
                if run_id == execution.run_id:
                    if event.event_id in execution.event_ids:
                        return
                execution.add_event(event)
                return
            raise RuntimeError('could not find appropriate subtask execution for '
                               'event: %s' % event)
