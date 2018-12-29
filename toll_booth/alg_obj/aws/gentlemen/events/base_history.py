"""
    The abstract/highest level classes for the activity and lambda_calls modules of AWS SWF
"""

import sys


class History:
    @classmethod
    def generate_from_events(cls, events):
        history = cls()
        current_module = sys.modules[cls.__module__]
        steps = getattr(current_module, '_steps')
        events = [x for x in events if x.event_type in steps['all']]
        operation_events = [x for x in events if x.event_type == steps['operation_first']]
        execution_events = [x for x in events if x.event_type == steps['execution_first']]
        failure_events = [x for x in events if x.event_type == steps['failure']]
        generic_events = [x for x in events if x not in operation_events or execution_events or failure_events]
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
    def flow_ids(self):
        operations = getattr(self, '_operations')
        return [x.flow_id for x in operations]

    @property
    def operations(self):
        return getattr(self, '_operations')

    def add_event(self, event):
        event_type = event.event_type
        add_operation_event = getattr(self, '_add_operation_event')
        add_execution_event = getattr(self, '_add_execution_event')
        add_failure_event = getattr(self, '_add_failure_event')
        add_general_event = getattr(self, '_add_general_event')
        current_module = sys.modules[self.__module__]
        steps = getattr(current_module, '_steps')
        if event_type == steps['operation_first']:
            return add_operation_event(event)
        if event_type == steps['execution_first']:
            return add_execution_event(event)
        if event_type == steps['failure']:
            return add_failure_event(event)
        return add_general_event(event)

    def merge_history(self, subtask_history):
        for new_operation in subtask_history.operations:
            if new_operation not in self.operations:
                self.operations.append(new_operation)

    def get_by_id(self, flow_id):
        return [x for x in self.operations if x.flow_id == flow_id]

    def get_by_name(self, name):
        return [x for x in self.operations if x.name == name]


class Operation:
    @property
    def flow_id(self):
        return getattr(self, '_flow_id')

    @property
    def name(self):
        return getattr(self, '_name')

    @property
    def executions(self):
        return getattr(self, '_executions')

    @property
    def results(self):
        returned_results = []
        for execution in self.executions:
            if execution.results:
                returned_results.append(execution.results)
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
    def event_ids(self):
        event_ids = []
        executions = getattr(self, '_executions')
        for execution in executions:
            event_ids.extend(execution.event_ids)
        return event_ids

    def add_execution(self, execution):
        executions = getattr(self, '_executions')
        executions.append(execution)

    def set_operation_failure(self, event):
        setattr(self, '_operation_failure', event)


class Execution:
    @property
    def run_id(self):
        return getattr(self, '_run_id')

    @property
    def status(self):
        events = getattr(self, '_events')
        time_sorted = sorted(events, key=lambda x: x.event_timestamp, reverse=True)
        return str(time_sorted[0])

    @property
    def event_ids(self):
        events = getattr(self, '_events')
        return [x.event_id for x in events]

    @property
    def is_completed(self):
        current_module = sys.modules[self.__module__]
        steps = getattr(current_module, '_steps')
        return self.status == steps['completed']

    @property
    def results(self):
        events = getattr(self, '_events')
        time_sorted = sorted(events, key=lambda x: x.event_timestamp, reverse=True)
        if time_sorted:
            try:
                return time_sorted[0].event_attributes['result']
            except KeyError:
                return None
        return None

    @property
    def is_live(self):
        current_module = sys.modules[self.__module__]
        steps = getattr(current_module, '_steps')
        return self.status in steps['live']

    @property
    def is_failed(self):
        current_module = sys.modules[self.__module__]
        steps = getattr(current_module, '_steps')
        return self.status in steps['failed']

    @property
    def fail_reason(self):
        current_module = sys.modules[self.__module__]
        steps = getattr(current_module, '_steps')
        if self.status in steps['failed']:
            return self.status
        return None

    def add_event(self, event):
        events = getattr(self, '_events')
        events.append(event)
        setattr(self, '_events', events)
