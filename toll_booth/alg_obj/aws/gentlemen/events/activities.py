"""
    these objects represent all the activities done in an AWS SWF workflow,
    they are collected together under the ActivityHistory object, as a collection of ActivityExecution objects
"""

from toll_booth.alg_obj.aws.gentlemen.events.base_history import Execution, Operation, History
from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.aws.gentlemen.tasks import TaskArguments

steps = {
    'execution_first': 'ActivityTaskStarted',
    'operation_first': 'ActivityTaskScheduled',
    'all': [
        'ActivityTaskScheduled',
        'ScheduleActivityTaskFailed',
        'ActivityTaskStarted',
        'ActivityTaskCompleted',
        'ActivityTaskFailed',
        'ActivityTaskTimedOut',
        'ActivityTaskCanceled',
        'ActivityTaskCancelRequested',
    ],
    'live': [
        'ActivityTaskStarted'
    ],
    'failed': [
        'ActivityTaskFailed',
        'ActivityTaskTimedOut',
        'ActivityTaskCanceled',
    ],
    'failure': 'ScheduleActivityTaskFailed',
    'completed': 'ActivityTaskCompleted'
}


class ActivityExecution(Execution):
    def __init__(self, execution_id: str, run_id: str, events: [Event]):
        super().__init__(execution_id, events, steps)
        self._run_id = run_id

    @classmethod
    def generate_from_start_event(cls, event: Event):
        activity_args = {
            'execution_id': event.event_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"),
            'run_id': event.event_id,
            'events': [event]
        }
        return cls(**activity_args)

    @property
    def run_id(self):
        return self._run_id


class ActivityOperation(Operation):
    def __init__(self, operation_id: str, run_ids: str, activity_name: str, activity_version: str, task_args: TaskArguments,
                 events):
        super().__init__(operation_id, run_ids, activity_name, activity_version, task_args, events, steps)

    @classmethod
    def generate_from_schedule_event(cls, event: Event):
        task_args = TaskArguments.from_schedule_event(event)
        cls_args = {
            'run_ids': [event.event_id],
            'operation_id': event.event_attributes['activityId'],
            'activity_name': event.event_attributes['activityType']['name'],
            'activity_version': event.event_attributes['activityType']['version'],
            'task_args': task_args,
            'events': [event]
        }
        return cls(**cls_args)

    @property
    def activity_name(self):
        return self._operation_name

    @property
    def activity_version(self):
        return self._operation_version

    @property
    def activity_executions(self):
        return self._executions


class ActivityHistory(History):
    def __init__(self, provided_steps=None,  operations: [ActivityOperation] = None):
        if not provided_steps:
            provided_steps = steps
        super().__init__(provided_steps, operations)

    def _add_operation_event(self, event: Event):
        new_operation_id = event.event_attributes['activityId']
        if new_operation_id in self.operation_ids:
            existing_operation = self.get(new_operation_id)
            existing_operation.add_run_id(event.event_id)
            return
        operation = ActivityOperation.generate_from_schedule_event(event)
        self._operations.append(operation)
        return

    def _add_execution_event(self, event: Event):
        run_id = event.event_attributes['scheduledEventId']
        execution = ActivityExecution.generate_from_start_event(event)
        for operation in self._operations:
            if run_id in operation.run_ids:
                if event.event_id in operation.event_ids:
                    return
                operation.add_execution(execution)
                return
        raise RuntimeError('could not find appropriate activity operation for activity execution: %s' % execution)

    def _add_failure_event(self, event: Event):
        operation_id = event.event_attributes['activityId']
        for operation in self._operations:
            if operation.operation_id == operation_id:
                operation.set_operation_failure(event)
                return
        raise RuntimeError('attempted to add a failure event to a non-existent activity operation')

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
        raise RuntimeError('could not find appropriate activity execution for event: %s' % event)
