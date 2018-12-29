from toll_booth.alg_obj.aws.gentlemen.events.activities import ActivityHistory
from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.aws.gentlemen.events.lambdas import LambdaHistory
from toll_booth.alg_obj.aws.gentlemen.events.markers import MarkerHistory
from toll_booth.alg_obj.aws.gentlemen.events.subtasks import SubtaskHistory

_starting_step = 'WorkflowExecutionStarted'


class WorkflowHistory:
    def __init__(self, domain_name: str, flow_type: str, task_token: str, flow_id: str, run_id: str, lambda_role: str,
                 input_str: str, events: [Event], subtask_history: SubtaskHistory, lambda_history: LambdaHistory,
                 activity_history: ActivityHistory, marker_history: MarkerHistory):
        self._domain_name = domain_name
        self._flow_type = flow_type
        self._flow_id = flow_id
        self._run_id = run_id
        self._lambda_role = lambda_role
        self._input_str = input_str
        self._task_token = task_token
        self._events = events
        self._lambda_history = lambda_history
        self._subtask_history = subtask_history
        self._activity_history = activity_history
        self._marker_history = marker_history

    @classmethod
    def parse_from_poll(cls, domain_name, poll_response, flow_type=None, task_token=None, flow_id=None, run_id=None):
        if not flow_type:
            flow_type = poll_response['workflowType']['name']
        if not task_token:
            task_token = poll_response['taskToken']
        execution_info = poll_response.get('workflowExecution', None)
        if not flow_id:
            flow_id = execution_info['workflowId']
        if not run_id:
            run_id = execution_info['runId']
        raw_events = poll_response['events']
        events = [Event.parse_from_decision_poll_event(x) for x in raw_events]
        lambda_history = LambdaHistory.generate_from_events(events)
        subtask_history = SubtaskHistory.generate_from_events(events)
        activity_history = ActivityHistory.generate_from_events(events)
        marker_history = MarkerHistory.generate_from_events(events)
        input_str, lambda_role = cls._generate_workflow_starter_data(events)
        cls_args = {
            'domain_name': domain_name, 'flow_type': flow_type, 'task_token': task_token, 'flow_id': flow_id,
            'run_id': run_id, 'input_str': input_str, 'lambda_role': lambda_role, 'events': events,
            'subtask_history': subtask_history, 'lambda_history': lambda_history, 'activity_history': activity_history,
            'marker_history': marker_history
        }
        return cls(**cls_args)

    @classmethod
    def _generate_workflow_starter_data(cls, events: [Event]):
        for event in events:
            if event.event_type == _starting_step:
                return event.event_attributes['input'], event.event_attributes['lambdaRole']

    @property
    def domain_name(self):
        return self._domain_name

    @property
    def events(self):
        return self._events

    @property
    def flow_type(self):
        return self._flow_type

    @property
    def flow_id(self):
        return self._flow_id

    @property
    def run_id(self):
        return self._run_id

    @property
    def task_token(self):
        return self._task_token

    @property
    def flow_input(self):
        return self._input_str

    @property
    def lambda_role(self):
        return self._lambda_role

    @property
    def subtask_history(self):
        return self._subtask_history

    @property
    def lambda_history(self):
        return self._lambda_history

    @property
    def activity_history(self):
        return self._activity_history

    @property
    def marker_history(self):
        return self._marker_history

    def get_lambda_operation(self, fn_name):
        return self._lambda_history.get_lambdas_by_name(fn_name)

    def get_subtask_operation(self, task_name):
        return self._subtask_history.get_subtasks_by_name(task_name)

    def merge_history(self, work_history):
        self._events.extend(work_history.events)
        self._subtask_history.merge_history(work_history.subtask_history)
        self._lambda_history.merge_history(work_history.lambda_history)
        self._activity_history.merge_history(work_history.activity_history)
        self._marker_history.merge_history(work_history.marker_history)
