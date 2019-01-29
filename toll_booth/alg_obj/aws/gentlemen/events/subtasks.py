from toll_booth.alg_obj.aws.gentlemen.events.base_history import History, Operation, Execution
from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.aws.gentlemen.tasks import TaskArguments

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
        self._flow_id = execution_id

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

    @property
    def flow_id(self):
        return self._flow_id


class SubtaskOperation(Operation):
    def __init__(self, operation_id, run_ids, task_name, task_version, task_args: TaskArguments, lambda_role,
                 task_list_name, events: [Event]):
        super().__init__(operation_id, run_ids, task_name, task_version, task_args, events, steps)
        self._task_list_name = task_list_name
        self._lambda_role = lambda_role

    @classmethod
    def generate_from_schedule_event(cls, event: Event):
        task_args = TaskArguments.from_schedule_event(event)
        cls_args = {
            'operation_id': event.event_attributes['workflowId'],
            'run_ids': [event.event_id],
            'task_name': event.event_attributes['workflowType']['name'],
            'task_version': event.event_attributes['workflowType']['version'],
            'task_args': task_args,
            'lambda_role': event.event_attributes['lambdaRole'],
            'task_list_name': event.event_attributes['taskList']['name'],
            'events': [event],
        }
        return cls(**cls_args)

    @property
    def task_name(self):
        return self._operation_name

    @property
    def task_version(self):
        return self._operation_version

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
        subtask_execution = SubtaskExecution.generate_from_start_event(event)
        operation_flow_id = subtask_execution.execution_id
        for operation in self._operations:
            if operation_flow_id == operation.operation_id:
                if event.event_id in operation.event_ids:
                    return
                operation.add_execution(subtask_execution)
                return
        raise RuntimeError('could not find appropriate subtask operation for subtask execution: %s' % subtask_execution)

    def _add_failure_event(self, event: Event):
        operation_id = event.event_attributes['workflowId']
        for operation in self._operations:
            if operation.operation_id == operation_id:
                operation.set_operation_failure(event)
                return
        raise RuntimeError('attempted to add a failure event to a non-existent subtask operation')

    def _add_general_event(self, event: Event):
        execution_run_id = event.event_attributes['startedEventId']
        operation_run_id = event.event_attributes['initiatedEventId']
        for operation in self._operations:
            if operation_run_id in operation.run_ids:
                for execution in operation.executions:
                    if execution_run_id == execution.execution_id:
                        if event.event_id in execution.event_ids:
                            return
                    execution.add_event(event)
                return
        raise RuntimeError('could not find appropriate subtask execution for event: %s' % event)
