import json
from json import JSONDecodeError

from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.serializers import AlgDecoder


class Marker:
    def __init__(self, run_id, marker_type: str, marker_details: dict):
        self._run_id = run_id
        self._marker_type = marker_type
        self._marker_details = marker_details

    @classmethod
    def parse_from_event(cls, run_id, event: Event):
        event_attributes = event.event_attributes
        marker_type = event_attributes['markerName']
        try:
            marker_details = json.loads(event_attributes['details'], cls=AlgDecoder)
        except JSONDecodeError:
            marker_details = event_attributes['details']
        return cls(run_id, marker_type, marker_details)

    @property
    def run_id(self):
        return self._run_id

    @property
    def marker_type(self):
        return self._marker_type

    @property
    def marker_details(self):
        return self._marker_details


class MarkerHistory:
    def __init__(self, markers: [Marker] = None):
        if not markers:
            markers = []
        self._markers = markers

    @classmethod
    def generate_from_events(cls, run_id, events):
        history = MarkerHistory()
        events = [x for x in events if x.event_type == 'MarkerRecorded']
        for event in events:
            marker = Marker.parse_from_event(run_id, event)
            history.add_marker(marker)
        return history

    @property
    def markers(self):
        return self._markers

    @property
    def names(self):
        names = {}
        name_markers = self.get_markers_by_type('names')
        for marker in name_markers:
            names.update(marker.marker_details)
        return names

    @property
    def checkpoints(self):
        checkpoints = {}
        check_markers = self.get_markers_by_type('checkpoint')
        for marker in check_markers:
            checkpoints.update(marker.marker_details)
        return checkpoints

    @property
    def config_marker(self):
        config_markers = self.get_markers_by_type('config')
        for marker in config_markers:
            return marker
        return None

    @property
    def versions_marker(self):
        value_markers = self.get_markers_by_type('versions')
        for marker in value_markers:
            return marker
        return None

    def get_signal_markers(self, run_id):
        signal_markers = self.get_markers_by_type('signal')
        return [x.marker_details for x in signal_markers if x.run_id == run_id]

    def get_ruffians(self, run_id):
        ruffians = {}
        ruffian_markers = self.get_markers_by_type('ruffian')
        run_ruffians = [x for x in ruffian_markers if x.run_id == run_id]
        for marker in run_ruffians:
            task_identifier = marker.marker_details['task_identifier']
            if task_identifier not in ruffians:
                ruffians[task_identifier] = []
            ruffians[task_identifier].append(marker.marker_details)
        return ruffians

    def get_open_ruffian_tasks(self, run_id):
        idlers = {}
        ruffians = self.get_ruffians(run_id)
        for task_identifier, ruffians in ruffians.items():
            for ruffian in ruffians:
                if ruffian['is_close'] is True:
                    continue
                idlers[task_identifier] = ruffian
        return idlers

    def add_marker(self, marker: Marker):
        self._markers.append(marker)

    def get_markers_by_type(self, marker_type):
        return [x for x in self._markers if x.marker_type == marker_type]

    def merge_history(self, marker_history):
        for new_marker in marker_history.markers:
            if new_marker not in self._markers:
                self._markers.append(new_marker)

    def __contains__(self, item):
        if self.get_markers_by_type(item):
            return True
        return False

    def __iter__(self):
        return iter(self._markers)
