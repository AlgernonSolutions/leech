from toll_booth.alg_obj.aws.gentlemen.events.base_history import History, Operation, Execution
from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.aws.gentlemen.tasks import TaskArguments

steps = {
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
    'failure': 'ScheduleLambdaFunctionFailed',
    'completed': 'LambdaFunctionCompleted'
}


class LambdaExecution(Execution):
    def __init__(self, execution_id: str, run_id: str, events: [Event]):
        super().__init__(execution_id, events, steps)
        self._run_id = run_id

    @classmethod
    def generate_from_start_event(cls, event: Event):
        run_id = event.event_attributes['scheduledEventId']
        execution_id: event.event_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
        return cls(execution_id, run_id, [event])


class LambdaOperation(Operation):
    def __init__(self, operation_id: str, run_ids: str, fn_name: str, task_args: TaskArguments, events: [Event]):
        super().__init__(operation_id, run_ids, fn_name, '1', task_args, events, steps)

    @classmethod
    def generate_from_schedule_event(cls, event: Event):
        task_args = TaskArguments.from_schedule_event(event)
        operation_args = {
            'fn_name': event.event_attributes['name'],
            'operation_id': event.event_attributes['id'],
            'run_ids': [event.event_id],
            'task_args': task_args,
            'events': [event]
        }
        return cls(**operation_args)

    @property
    def fn_name(self):
        return self._operation_name

    @property
    def lambda_executions(self):
        return self._executions


class LambdaHistory(History):
    def __init__(self, provided_steps=None, operations: [LambdaOperation] = None):
        if not provided_steps:
            provided_steps = steps
        super().__init__(provided_steps, operations)

    def _add_operation_event(self, event: Event):
        if event.event_attributes['id'] in self.operation_ids:
            return
        lambda_operation = LambdaOperation.generate_from_schedule_event(event)
        self._operations.append(lambda_operation)
        return

    def _add_execution_event(self, event: Event):
        run_id = event.event_attributes['scheduledEventId']
        lambda_execution = LambdaExecution.generate_from_start_event(event)
        for operation in self._operations:
            if run_id in operation.run_ids:
                if event.event_id in operation.event_ids:
                    return
                operation.add_execution(lambda_execution)
                return
        raise RuntimeError('could not find appropriate lambda operation for lambda execution: %s' % lambda_execution)

    def _add_failure_event(self, event: Event):
        operation_id = event.event_attributes['id']
        for operation in self._operations:
            if operation.operation_id == operation_id:
                operation.set_operation_failure(event)
                return
        raise RuntimeError('attempted to add a failure event to a non-existent lambda operation')

    def _add_general_event(self, event: Event):
        operation_run_id = event.event_attributes['scheduledEventId']
        execution_run_id = event.event_attributes['startedEventId']
        for operation in self._operations:
            if operation_run_id in operation.run_ids:
                for execution in operation.executions:
                    if execution_run_id == execution.run_id:
                        if event.event_id in execution.event_ids:
                            return
                    execution.add_event(event)
                    return
        raise RuntimeError('could not find appropriate lambda execution for event: %s' % event)
