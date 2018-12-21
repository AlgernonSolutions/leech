_lambda_steps = {
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
    ]
}
_subtask_steps = {
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
    ]

}


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
'ScheduleLambdaFunctionFailed'
'StartLambdaFunctionFailed'


class EventHistory:
    def __init__(self, events=None, events_by_type=None):
        if not events:
            events = []
        if not events_by_type:
            events_by_type = {}
        self._events = events
        self._events_by_type = events_by_type

    @classmethod
    def generate_from_events(cls, events: [Event]):
        event_types = {}
        for event in events:
            event_type = event.event_type
            if event_type not in event_types:
                event_types[event_type] = []
            event_types[event_type].append(event)
        return cls(events, event_types)

    @property
    def events(self):
        return self._events

    @property
    def events_by_type(self):
        return self._events_by_type

    def add_events(self, events: [Event]):
        for event in events:
            self.add_event(event)

    def add_event(self, event: Event):
        event_type = event.event_type
        if event_type not in self._events_by_type:
            self._events_by_type[event_type] = []
        self._events_by_type[event_type].append(event)
        self._events.append(event)

    def get_event_by_type(self, event_type):
        return self._events_by_type.get(event_type, None)

    def get_lambda_sets(self, fn_name_filter=None):
        return LambdaEventSets.strain_from_events(self._events, fn_name_filter)

    def get_subtask_sets(self):
        return SubtaskEventSets.strain_from_events(self._events)

    def __iter__(self):
        return iter(self._events)


class SubtaskEventSet:
    def __init__(self, execution_id, events: EventHistory = None):
        if not events:
            events = EventHistory()
        self._execution_id = execution_id
        self._events = events

    @classmethod
    def strain_from_history(cls, target_id, event_history: EventHistory):
        pass

    @property
    def execution_id(self):
        return self._execution_id

    @property
    def status(self):
        time_sorted = sorted(self._events.events, key=lambda x: x.event_timestamp, reverse=True)
        return str(time_sorted[0])

    @property
    def is_live(self):
        return self.status in _subtask_steps['live']

    @property
    def is_failed(self):
        return self.status in _subtask_steps['failed']

    @property
    def is_completed(self):
        return self.status == 'ChildWorkflowExecutionCompleted'

    @property
    def fail_reason(self):
        if self.status in _subtask_steps['failed']:
            return self.status
        return None

    @property
    def results(self):
        time_sorted = sorted(self._events.events, key=lambda x: x.event_timestamp, reverse=True)
        try:
            return time_sorted[0].event_attributes['result']
        except KeyError:
            return None

    def add_event(self, event):
        self._events.add_event(event)


class SubtaskEventSets:
    def __init__(self, event_sets: [SubtaskEventSet]):
        self._event_sets = event_sets

    def __bool__(self):
        return bool(self._event_sets)

    @classmethod
    def strain_from_events(cls, events: [Event]):
        subtask_events = {}
        for event in events:
            if 'ChildWorkflow' in event.event_type:
                try:
                    flow_id = event.event_attributes['workflowExecution']['workflowId']
                except KeyError:
                    flow_id = event.event_attributes['workflowId']
                if flow_id not in subtask_events:
                    subtask_events[flow_id] = SubtaskEventSet(flow_id)
                subtask_events[flow_id].add_event(event)
        return cls(list(subtask_events.values()))

    def get_for_task_name(self, task_name):
        return [x for x in self._event_sets if task_name in x.execution_id]


class LambdaEventSet:
    def __init__(self, execution_id, fn_name, events: EventHistory = None):
        if not events:
            events = EventHistory()
        self._execution_id = execution_id
        self._fn_name = fn_name
        self._events = events

    @classmethod
    def strain_from_history(cls, target_id, event_history: EventHistory):
        lambda_events = []
        fn_name = None
        for event in event_history:
            if event.event_type in _lambda_steps['all']:
                lambda_id = event.event_attributes['id']
                if lambda_id == target_id:
                    if not fn_name:
                        fn_name = event.event_attributes['name']
                    lambda_events.append(event)
        if lambda_events:
            return cls(target_id, fn_name, EventHistory.generate_from_events(lambda_events))
        return None

    @property
    def execution_id(self):
        return self._execution_id

    @property
    def fn_name(self):
        return self._fn_name

    @property
    def status(self):
        time_sorted = sorted(self._events.events, key=lambda x: x.event_timestamp, reverse=True)
        return str(time_sorted[0])

    @property
    def is_live(self):
        return self.status in _lambda_steps['live']

    @property
    def is_failed(self):
        return self.status in _lambda_steps['failed']

    @property
    def is_completed(self):
        return self.status == 'LambdaFunctionCompleted'

    @property
    def fail_reason(self):
        if self.status in _lambda_steps['failed']:
            return self.status
        return None

    @property
    def results(self):
        time_sorted = sorted(self._events.events, key=lambda x: x.event_timestamp, reverse=True)
        return time_sorted[0].event_attributes['result']

    def add_lambda_event(self, lambda_event):
        self._events.add_event(lambda_event)


class LambdaEventSets:
    def __init__(self, event_sets: [LambdaEventSet]):
        self._event_sets = event_sets

    def __bool__(self):
        return bool(self._event_sets)

    def get_sets_by_name(self, fn_name):
        return [x for x in self._event_sets if x.fn_name == fn_name]

    @classmethod
    def strain_from_events(cls, events: [Event], fn_name_filter=None):
        lambda_event_groups = {}
        time_sorted = sorted(events, key=lambda x: x.event_id)
        for event in time_sorted:
            if event.event_type == 'LambdaFunctionScheduled':
                fn_name = event.event_attributes.get('name', None)
                lambda_id = event.event_attributes['id']
                pointer = time_sorted.index(event)
                if fn_name_filter:
                    if fn_name and fn_name != fn_name_filter:
                        continue
                if lambda_id not in lambda_event_groups:
                    lambda_event_groups[lambda_id] = LambdaEventSet(lambda_id, fn_name)
                lambda_event_groups[lambda_id].add_lambda_event(event)
                while pointer:
                    pointer += 1
                    next_event = time_sorted[pointer]
                    if 'Lambda' not in next_event.event_type:
                        break
                    lambda_event_groups[lambda_id].add_lambda_event(next_event)
        return cls(list(lambda_event_groups.values()))


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
