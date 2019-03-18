import json

from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.serializers import AlgDecoder


class WorkflowSignal:
    def __init__(self, run_id, signal_name, input_string, **kwargs):
        self._run_id = run_id
        self._signal_name = signal_name
        self._input_string = input_string
        self._signaling_flow_id = kwargs.get('signaling_flow_id')
        self._signaling_run_id = kwargs.get('signaling_run_id')

    @classmethod
    def parse_from_event(cls, run_id, event: Event):
        event_attributes = event.event_attributes
        signaling_execution = event_attributes.get('externalWorkflowExecution', {})
        cls_kwargs = {
            'run_id': run_id,
            'signal_name': event_attributes['signalName'],
            'input_string': event_attributes['input'],
            'signaling_flow_id': signaling_execution.get('workflowId'),
            'signaling_run_id': signaling_execution.get('runId')
        }
        return cls(**cls_kwargs)

    @property
    def run_id(self):
        return self._run_id

    @property
    def signal_name(self):
        return self._signal_name

    @property
    def input_string(self):
        return self._input_string

    @property
    def input_value(self):
        return json.loads(self._input_string, cls=AlgDecoder)

    @property
    def signaling_flow_id(self):
        return self._signaling_flow_id

    @property
    def signaling_run_id(self):
        return self._signaling_run_id


class WorkflowSignalHistory:
    def __init__(self, signals: [WorkflowSignal] = None):
        if not signals:
            signals = []
        self._signals = signals

    @classmethod
    def generate_from_events(cls, run_id, events):
        history = WorkflowSignalHistory()
        signal_events = [x for x in events if x.event_type == 'WorkflowExecutionSignaled']
        for event in signal_events:
            workflow_signal = WorkflowSignal.parse_from_event(run_id, event)
            history.add_workflow_signal(workflow_signal)
        return history

    @property
    def signals(self):
        return self._signals

    @property
    def ruffian_signals(self):
        signals = []
        for entry in self._signals:
            if entry.signal_name in ['start_ruffian', 'stop_ruffian']:
                signal = entry.input_value
                signal['signal_name'] = entry.signal_name
                signals.append(signal)
        return signals

    def add_workflow_signal(self, workflow_signal):
        self._signals.append(workflow_signal)

    def get_by_signal_name(self, signal_name):
        return [x for x in self._signals if x.signal_name == signal_name]

    def __iter__(self):
        return iter(self._signals)

    def __contains__(self, item):
        if self.get_by_signal_name(item):
            return True
        return False
