import json

from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.serializers import AlgDecoder


class Timer:
    def __init__(self, timer_id: str, timer_name: str, duration_seconds: int, details: object):
        self._timer_id = timer_id
        self._timer_name = timer_name
        self._duration_seconds = duration_seconds
        self._details = details
        self._is_fired = False

    @property
    def timer_id(self):
        return self._timer_id

    @property
    def timer_name(self):
        return self._timer_name

    @property
    def duration_seconds(self):
        return self._duration_seconds

    @property
    def details(self):
        return self._details

    @property
    def is_fired(self):
        return self._is_fired is not False

    @classmethod
    def parse_from_events(cls, event: Event):
        event_attributes = event.event_attributes
        timer_id = event_attributes['timerId']
        timer_name = timer_id.split('!')[0]
        control = event_attributes['control']
        timeout = event_attributes['startToFireTimeout']
        details = json.loads(control, cls=AlgDecoder)
        return cls(timer_id, timer_name, int(timeout), details)

    def set_timer_fired(self, event: Event):
        self._is_fired = event.event_id


class TimerHistory:
    def __init__(self, timers: [Timer] = None):
        if not timers:
            timers = []
        self._timers = timers

    @property
    def timers(self):
        return self._timers

    @property
    def timer_ids(self):
        return [x.timer_id for x in self._timers]

    @property
    def details(self):
        return [x.details for x in self._timers]

    @property
    def backed_off(self):
        return [x for x in self._timers if x.timer_name == 'error_back_off']

    @classmethod
    def generate_from_events(cls, events: [Event]):
        history = TimerHistory()
        start_events = [x for x in events if x.event_type == 'TimerStarted']
        fire_events = [x for x in events if x.event_type == 'TimerFired']
        for event in start_events:
            timer = Timer.parse_from_events(event)
            history.add_timer(timer)
        for event in fire_events:
            timer_id = event.event_attributes['timerId']
            history[timer_id].set_timer_fired(event)
        return history

    def __getitem__(self, item):
        for timer in self._timers:
            if timer.timer_id == item:
                return timer
        raise KeyError(item)

    def __contains__(self, item):
        return item in self.timer_ids

    def __iter__(self):
        return iter(self._timers)

    def merge_history(self, new_history):
        for timer in new_history.timers:
            self.add_timer(timer)

    def add_timer(self, timer: Timer):
        self._timers.append(timer)

    def fn_back_off_status(self, fn_identifier):
        backed_off = self.backed_off
        fn_backed_off = [x for x in backed_off if x.details['fn_identifier'] == fn_identifier]
        for timer in fn_backed_off:
            if not timer.is_fired:
                return True
        return len(fn_backed_off)

    def is_fn_backed_off(self, fn_identifier):
        return self.fn_back_off_status(fn_identifier) is True
