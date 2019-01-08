"""
    The abstract/highest level classes for the activity and lambda_calls modules of AWS SWF
"""

from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.aws.gentlemen.tasks import TaskArguments


class History:
    def __init__(self, steps, operations=None):
        if not operations:
            operations = []
        self._operations = operations
        self._steps = steps

    @classmethod
    def generate_from_events(cls, events, steps):
        history = cls(steps)
        events = [x for x in events if x.event_type in steps['all']]
        operation_events = [x for x in events if x.event_type == steps['operation_first']]
        execution_events = [x for x in events if x.event_type == steps['execution_first']]
        failure_events = [x for x in events if x.event_type == steps['failure']]
        special_event_ids = []
        for special_events in [operation_events, execution_events, failure_events]:
            special_event_ids.extend([x.event_id for x in special_events])
        generic_events = [x for x in events if x.event_id not in special_event_ids]
        for operation_event in operation_events:
            history.add_event(operation_event)
        for execution_event in execution_events:
            history.add_event(execution_event)
        for failure_event in failure_events:
            history.add_event(failure_event)
        for generic_event in generic_events:
            history.add_event(generic_event)
        return history

    @property
    def operation_ids(self):
        return [x.operation_id for x in self._operations]

    @property
    def operations(self):
        return self._operations

    @property
    def operation_names(self):
        return [x.operation_name for x in self._operations]

    def add_event(self, event):
        event_type = event.event_type
        add_operation_event = getattr(self, '_add_operation_event')
        add_execution_event = getattr(self, '_add_execution_event')
        add_failure_event = getattr(self, '_add_failure_event')
        add_general_event = getattr(self, '_add_general_event')
        if event_type == self._steps['operation_first']:
            return add_operation_event(event)
        if event_type == self._steps['execution_first']:
            return add_execution_event(event)
        if event_type == self._steps['failure']:
            return add_failure_event(event)
        return add_general_event(event)

    def merge_history(self, subtask_history):
        for new_operation in subtask_history.operations:
            if new_operation.operation_id not in self.operation_ids:
                self.operations.append(new_operation)
                continue
            for operation in self._operations:
                if new_operation.operation_id == operation.operation_id:
                    operation.add_run_ids(new_operation.run_ids)
                    for new_execution in new_operation.executions:
                        if new_execution.execution_id not in operation.execution_ids:
                            operation.add_execution(new_execution)
                            continue
                        continue

    def get_by_id(self, operation_id):
        return [x for x in self.operations if x.operation_id == operation_id]

    def get_by_name(self, operation_name):
        return [x for x in self.operations if x.operation_name == operation_name]

    def get_operation_failed_count(self, operation_id, fail_reason=None):
        failed_count = 0
        operations = self.get_by_id(operation_id)
        for operation in operations:
            for execution in operation.executions:
                if execution.is_failed:
                    if fail_reason:
                        if execution.fail_reason != fail_reason:
                            continue
                    failed_count += 1
        return failed_count

    def get_result(self, operation_name):
        operation = self[operation_name]
        return operation.results

    def get_result_value(self, operation_name):
        import json
        from toll_booth.alg_obj.serializers import AlgDecoder
        results = self.get_result(operation_name)
        operation_results = json.loads(results, cls=AlgDecoder)
        return operation_results.data_string

    def __contains__(self, item):
        return item in self._operations

    def __getitem__(self, item):
        for operation in self._operations:
            if operation.operation_id == item:
                return operation
        raise AttributeError('operation named: %s is not present in this history' % item)

    def __iter__(self):
        return iter(self._operations)

    def get(self, item, default=None):
        try:
            return self[item]
        except AttributeError:
            return default


class Operation:
    def __init__(self, operation_id: str, run_ids: [str], operation_name: str, operation_version: str, task_args: TaskArguments, events, steps):
        self._operation_id = operation_id
        self._run_ids = run_ids
        self._operation_name = operation_name
        self._operation_version = operation_version
        self._task_args = task_args
        self._events = events
        self._executions = []
        self._steps = steps
        self._operation_failure = None

    @property
    def operation_id(self):
        return self._operation_id

    @property
    def run_ids(self):
        return self._run_ids

    @property
    def operation_name(self):
        return self._operation_name

    @property
    def task_args(self):
        return self._task_args

    @property
    def executions(self):
        return self._executions

    @property
    def execution_ids(self):
        return [x.execution_id for x in self._executions]

    @property
    def results(self):
        returned_results = set()
        for execution in self.executions:
            if execution.results:
                returned_results.add(execution.results)
        if len(returned_results) > 1:
            raise RuntimeError('multiple invocations of the same workflow with the same input '
                               'must return the same values, this was not the case')
        for result in returned_results:
            return result
        return None

    @property
    def is_live(self):
        for execution in self.executions:
            if execution.is_live:
                return True
        return False

    @property
    def is_complete(self):
        for execution in self.executions:
            if execution.is_completed:
                return True
        return False

    @property
    def is_failed(self):
        if self.is_complete:
            return False
        if self.is_live:
            return False
        for execution in self._executions:
            if execution.is_failed:
                return True
        return False

    @property
    def event_ids(self):
        event_ids = []
        for execution in self._executions:
            event_ids.extend(execution.event_ids)
        return event_ids

    def add_execution(self, execution):
        self._executions.append(execution)

    def add_run_id(self, run_id):
        self._run_ids.append(run_id)

    def add_run_ids(self, run_ids):
        for run_id in run_ids:
            if run_id not in self._run_ids:
                self._run_ids.append(run_id)

    def set_operation_failure(self, event):
        self._operation_failure = event

    def __str__(self):
        return self._operation_id


class Execution:
    def __init__(self, execution_id: str, events: [Event], steps):
        self._execution_id = execution_id
        self._events = events
        self._steps = steps

    @property
    def execution_id(self):
        return self._execution_id

    @property
    def status(self):
        time_sorted = sorted(self._events, key=lambda x: x.event_timestamp, reverse=True)
        return str(time_sorted[0])

    @property
    def event_ids(self):
        return [x.event_id for x in self._events]

    @property
    def is_completed(self):
        return self.status == self._steps['completed']

    @property
    def results(self):
        time_sorted = sorted(self._events, key=lambda x: x.event_timestamp, reverse=True)
        if time_sorted:
            try:
                return time_sorted[0].event_attributes['result']
            except KeyError:
                return None
        return None

    @property
    def is_live(self):
        return self.status in self._steps['live']

    @property
    def is_failed(self):
        return self.status in self._steps['failed']

    @property
    def fail_reason(self):
        if self.status in self._steps['failed']:
            return self.status
        return None

    def add_event(self, event):
        self._events.append(event)

    def __str__(self):
        return self._execution_id
