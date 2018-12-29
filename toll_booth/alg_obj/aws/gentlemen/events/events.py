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
