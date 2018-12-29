"""
    these objects represent all the activities done in an AWS SWF workflow,
    they are collected together under the ActivityHistory object, as a collection of ActivityExecution objects
"""

from toll_booth.alg_obj.aws.gentlemen.events.events import Event

_activity_steps = {
    'execution_first': 'ActivityTaskScheduled',
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


class ActivityExecution:
    def __init__(self, activity_type: str, activity_version: str, activity_id: str, input_string: str,
                 schedule_event_id: str, task_list_name: str, events: [Event]):
        self._activity_type = activity_type
        self._activity_version = activity_version
        self._activity_id = activity_id
        self._input_string = input_string
        self._schedule_event_id = schedule_event_id
        self._task_list_name = task_list_name
        self._events = events
        self._operation_failure = None

    @classmethod
    def generate_from_schedule_event(cls, event: Event):
        activity_args = {
            'activity_type': event.event_attributes['activityType']['name'],
            'activity_id': event.event_attributes['activityId'],
            'activity_version': event.event_attributes['activityType']['version'],
            'input_string': event.event_attributes['input'],
            'schedule_event_id': event.event_id,
            'task_list_name': event.event_attributes['taskList']['name'],
            'events': [event]
        }
        return cls(**activity_args)

    @property
    def activity_type(self):
        return self._activity_type

    @property
    def activity_version(self):
        return self._activity_version

    @property
    def activity_id(self):
        return self._activity_id

    @property
    def input_string(self):
        return self._input_string

    @property
    def schedule_event_id(self):
        return self._schedule_event_id

    @property
    def task_list_name(self):
        return self._task_list_name

    @property
    def status(self):
        time_sorted = sorted(self._events, key=lambda x: x.event_timestamp, reverse=True)
        return str(time_sorted[0])

    @property
    def is_live(self):
        return self.status in _activity_steps['live']

    @property
    def is_failed(self):
        return self.status in _activity_steps['failed']

    @property
    def is_complete(self):
        return self.status == _activity_steps['completed']

    @property
    def fail_reason(self):
        if self.status in _activity_steps['failed']:
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

    def set_operation_failure(self, event):
        self._operation_failure = event

    def add_event(self, event: Event):
        self._events.append(event)


class ActivityHistory:
    def __init__(self, executions: [ActivityExecution] = None):
        if not executions:
            executions = []
        self._executions = executions

    def __contains__(self, item):
        activities = self.get_activity_type_by_id(item)
        if activities:
            return True
        return False

    def __getitem__(self, item):
        activities = self.get_activity_type_by_id(item)
        if len(activities) > 1:
            raise RuntimeError('activity history contains multiple entries for the same activity_id')
        for activity in activities:
            return activity

    @classmethod
    def generate_from_events(cls, events: [Event]):
        history = ActivityHistory()
        events = [x for x in events if x.event_type in _activity_steps['all']]
        execution_events = [x for x in events if x.event_type == _activity_steps['execution_first']]
        other_events = [x for x in events if x not in execution_events]
        for execution_event in execution_events:
            activity_execution = ActivityExecution.generate_from_schedule_event(execution_event)
            history.add_execution(activity_execution)
        for event in other_events:
            history.add_event(event)
        return history

    @property
    def executions(self):
        return self._executions

    def get_activity_type_by_name(self, activity_name):
        activities = []
        for execution in self._executions:
            if execution.activity_type == activity_name:
                activities.append(execution)
        return activities

    def get_activity_type_by_id(self, activity_id):
        activities = []
        for execution in self._executions:
            if execution.activity_id == activity_id:
                activities.append(execution)
        return activities

    def get_activity_result(self, activity_name):
        executions = self.get_activity_type_by_name(activity_name)
        returned_results = []
        for execution in executions:
            if execution.results:
                returned_results.append(execution.results)
        if len(returned_results) > 1:
            raise RuntimeError('multiple invocations of the same task with the same input '
                               'must return the same values, this was not the case')
        for result in returned_results:
            return result
        return None

    def is_activity_live(self, activity_id):
        executions = self.get_activity_type_by_id(activity_id)
        for execution in executions:
            if execution.is_live:
                return True
        return False

    def is_activity_completed(self, activity_id):
        executions = self.get_activity_type_by_id(activity_id)
        for execution in executions:
            if execution.is_complete:
                return True
        return False

    def get_activity_failed_count(self, activity_id, fail_reason=None):
        count = 0
        executions = self.get_activity_type_by_id(activity_id)
        for execution in executions:
            if execution.is_failed:
                if fail_reason:
                    if execution.fail_reason != fail_reason:
                        continue
                count += 1
        return count

    def add_event(self, event: Event):
        if event.event_type == _activity_steps['failure']:
            activity_id = event.event_attributes['activityId']
            for execution in self._executions:
                if activity_id == execution.activity_id:
                    execution.set_operation_failure(event)
                    return
        for execution in self._executions:
            if execution.schedule_event_id == event.event_attributes['scheduledEventId']:
                execution.add_event(event)
                return
        raise RuntimeError('could not find a corresponding execution for event: %s ' % event)

    def add_execution(self, activity_execution: ActivityExecution):
        self._executions.append(activity_execution)

    def add_executions(self, activity_executions: [ActivityExecution]):
        self._executions.extend(activity_executions)

    def merge_history(self, activity_history):
        for new_execution in activity_history.executions:
            if new_execution not in self._executions:
                self._executions.append(new_execution)
