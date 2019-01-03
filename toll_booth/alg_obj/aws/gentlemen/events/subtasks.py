from toll_booth.alg_obj.aws.gentlemen.events.base_history import History, Operation, Execution
from toll_booth.alg_obj.aws.gentlemen.events.events import Event

steps = {
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
    def __init__(self, execution_id: str, run_id: str, events: [Event]):
        super().__init__(execution_id, events, steps)
        self._run_id = run_id

    @classmethod
    def generate_from_start_event(cls, event: Event):
        cls_args = {
            'execution_id': event.event_attributes['workflowExecution']['workflowId'],
            'run_id': event.event_attributes['workflowExecution']['runId'],
            'events': [event]
        }
        return cls(**cls_args)

    @property
    def run_id(self):
        return self._run_id


class SubtaskOperation(Operation):
    def __init__(self, operation_id: str, task_name: str, task_version: str, task_args: str, lambda_role: str, task_list_name: str,  events: [Event]):
        super().__init__(operation_id, task_name, task_version, task_args, events, steps)
        self._task_list_name = task_list_name
        self._lambda_role = lambda_role

    @classmethod
    def generate_from_schedule_event(cls, event: Event):
        cls_args = {
            'operation_id': event.event_attributes['workflowId'],
            'task_name': event.event_attributes['workflowType']['name'],
            'task_version': event.event_attributes['workflowType']['version'],
            'task_args': event.event_attributes['input'],
            'lambda_role': event.event_attributes['lambdaRole'],
            'task_list_name': event.event_attributes['taskList']['name'],
            'events': [event]
        }
        return cls(**cls_args)

    @property
    def task_name(self):
        return self._operation_name

    @property
    def task_version(self):
        return self._operation_version

    @property
    def task_args(self):
        return self._operation_input

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
    def __init__(self, provided_steps=None, operations: [SubtaskOperation] = None):
        if not provided_steps:
            provided_steps = steps
        super().__init__(provided_steps, operations)

    def _add_operation_event(self, event: Event):
        if event.event_attributes['workflowId'] in self.operation_ids:
            return
        subtask_operation = SubtaskOperation.generate_from_schedule_event(event)
        self._operations.append(subtask_operation)
        return

    def _add_execution_event(self, event: Event):
        operation_run_id = event.event_attributes['initiatedEventId']
        subtask_execution = SubtaskExecution.generate_from_start_event(event)
        for operation in self._operations:
            if operation_run_id == operation.run_id:
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
        execution_run_id = event.event_attributes['startedEventId']
        operation_run_id = event.event_attributes['initiatedEventId']
        for operation in self._operations:
            if operation_run_id == operation.run_id:
                for execution in operation.lambda_executions:
                    if execution_run_id == execution.run_id:
                        if event.event_id in execution.event_ids:
                            return
                    execution.add_event(event)
                return
        raise RuntimeError('could not find appropriate subtask execution for event: %s' % event)
