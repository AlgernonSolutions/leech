import json

from toll_booth.alg_obj.aws.gentlemen.events import lambdas, subtasks, activities
from toll_booth.alg_obj.aws.gentlemen.events.activities import ActivityHistory
from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.aws.gentlemen.events.lambdas import LambdaHistory
from toll_booth.alg_obj.aws.gentlemen.events.markers import MarkerHistory
from toll_booth.alg_obj.aws.gentlemen.events.subtasks import SubtaskHistory
from toll_booth.alg_obj.aws.gentlemen.events.timers import TimerHistory
from toll_booth.alg_obj.aws.gentlemen.tasks import TaskArguments
from toll_booth.alg_obj.serializers import AlgDecoder

_starting_step = 'WorkflowExecutionStarted'


class WorkflowHistory:
    def __init__(self, domain_name, flow_type, task_token, flow_id, run_id, lambda_role, parent_flow_id, parent_run_id,
                 task_args: TaskArguments, events: [Event], subtask_history: SubtaskHistory, lambda_history: LambdaHistory,
                 activity_history: ActivityHistory, marker_history: MarkerHistory, timer_history: TimerHistory):
        self._domain_name = domain_name
        self._flow_type = flow_type
        self._flow_id = flow_id
        self._run_id = run_id
        self._lambda_role = lambda_role
        self._parent_flow_id = parent_flow_id
        self._parent_run_id = parent_run_id
        self._task_args = task_args
        self._task_token = task_token
        self._events = events
        self._lambda_history = lambda_history
        self._subtask_history = subtask_history
        self._activity_history = activity_history
        self._marker_history = marker_history
        self._timer_history = timer_history

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
        if not raw_events:
            raise RuntimeError(f'no events were returned from a poll for work history')
        events = [Event.parse_from_decision_poll_event(x) for x in raw_events]
        lambda_history = LambdaHistory.generate_from_events(events, lambdas.steps)
        subtask_history = SubtaskHistory.generate_from_events(events, subtasks.steps)
        activity_history = ActivityHistory.generate_from_events(events, activities.steps)
        marker_history = MarkerHistory.generate_from_events(events)
        timer_history = TimerHistory.generate_from_events(events)
        task_args, lambda_role, parent_flow_id, parent_run_id = cls._generate_workflow_starter_data(flow_type, events)
        cls_args = {
            'domain_name': domain_name, 'flow_type': flow_type, 'task_token': task_token, 'flow_id': flow_id,
            'run_id': run_id, 'parent_flow_id': parent_flow_id, 'parent_run_id': parent_run_id, 'task_args': task_args,
            'lambda_role': lambda_role, 'events': events, 'subtask_history': subtask_history,
            'lambda_history': lambda_history, 'activity_history': activity_history,
            'marker_history': marker_history, 'timer_history': timer_history
        }
        return cls(**cls_args)

    @classmethod
    def _generate_workflow_starter_data(cls, operation_name, events: [Event]):
        for event in events:
            if event.event_type == _starting_step:
                input_string = event.event_attributes.get('input', '{}')
                task_args = TaskArguments.for_starting_data(operation_name, json.loads(input_string, cls=AlgDecoder))
                parent_data = event.event_attributes.get('parentWorkflowExecution', {})
                parent_flow_id = parent_data.get('workflowId', None)
                parent_run_id = parent_data.get('runId', None)
                return task_args, event.event_attributes['lambdaRole'], parent_flow_id, parent_run_id

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
    def has_parent(self):
        return self._parent_flow_id is not None

    @property
    def parent_flow_id(self):
        return self._parent_flow_id

    @property
    def parent_run_id(self):
        return self._parent_run_id

    @property
    def task_token(self):
        return self._task_token

    @property
    def task_args(self):
        return self._task_args

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

    @property
    def timer_history(self):
        return self._timer_history

    def merge_history(self, work_history):
        self._events.extend(work_history.events)
        self._subtask_history.merge_history(work_history.subtask_history)
        self._lambda_history.merge_history(work_history.lambda_history)
        self._activity_history.merge_history(work_history.activity_history)
        self._marker_history.merge_history(work_history.marker_history)
        self._timer_history.merge_history(work_history.timer_history)

    def get_result(self, identifier):
        try:
            results = self._activity_history.get_result_value(identifier)
        except AttributeError:
            try:
                results = self._subtask_history.get_result_value(identifier)
            except AttributeError:
                results = self._get_marker_result(identifier)
        if results is None:
            return results
        try:
            return json.loads(results, cls=AlgDecoder).data_string
        except TypeError:
            return results

    def _get_marker_result(self, name):
        checkpoints = self._marker_history.checkpoints
        return checkpoints[name]
