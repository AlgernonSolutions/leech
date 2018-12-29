from toll_booth.alg_obj.aws.gentlemen.events.base_history import History, Operation, Execution
from toll_booth.alg_obj.aws.gentlemen.events.events import Event

_steps = {
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
    def __init__(self, run_id: str, events: [Event]):
        self._run_id = run_id
        self._events = events

    @classmethod
    def generate_from_start_event(cls, event: Event):
        run_id = event.event_attributes['scheduledEventId']
        return cls(run_id, [event])


class LambdaOperation(Operation):
    def __init__(self, fn_name: str, flow_id: str, run_id: str, task_args: str,
                 schedule_event_id: str, lambda_executions: [LambdaExecution]):
        self._name = fn_name
        self._flow_id = flow_id
        self._run_id = run_id
        self._task_args = task_args
        self._schedule_event_id = schedule_event_id
        self._executions = lambda_executions
        self._operation_failure = None

    @classmethod
    def generate_from_schedule_event(cls, event: Event):
        operation_args = {
            'fn_name': event.event_attributes['name'],
            'flow_id': event.event_attributes['id'],
            'run_id': event.event_id,
            'task_args': event.event_attributes['input'],
            'schedule_event_id': event.event_id,
            'lambda_executions': [event]
        }
        return cls(**operation_args)

    @property
    def fn_name(self):
        return self._name

    @property
    def run_id(self):
        return self._run_id

    @property
    def schedule_event_id(self):
        return self._schedule_event_id

    @property
    def lambda_executions(self):
        return self._executions


class LambdaHistory(History):
    def __init__(self, operations: [LambdaOperation] = None):
        if not operations:
            operations = []
        self._operations = operations

    def _add_operation_event(self, event: Event):
        if event.event_attributes['id'] in self.flow_ids:
            return
        lambda_operation = LambdaOperation.generate_from_schedule_event(event)
        self._operations.append(lambda_operation)
        return

    def _add_execution_event(self, event: Event):
        run_id = event.event_attributes['scheduledEventId']
        lambda_execution = LambdaExecution.generate_from_start_event(event)
        for operation in self._operations:
            if run_id == operation.run_id:
                if event.event_id in operation.event_ids:
                    return
                operation.add_execution(lambda_execution)
                return
            raise RuntimeError('could not find appropriate lambda operation for '
                               'lambda execution: %s' % lambda_execution)

    def _add_failure_event(self, event: Event):
        flow_id = event.event_attributes['id']
        for operation in self._operations:
            if operation.flow_id == flow_id:
                operation.set_operation_failure(event)
                return
        raise RuntimeError('attempted to add a failure event to a non-existent lambda operation')

    def _add_general_event(self, event: Event):
        run_id = event.event_attributes['scheduledEventId']
        for operation in self._operations:
            for execution in operation.lambda_executions:
                if run_id == execution.run_id:
                    if event.event_id in execution.event_ids:
                        return
                execution.add_event(event)
                return
            raise RuntimeError('could not find appropriate lambda execution for '
                               'event: %s' % event)
