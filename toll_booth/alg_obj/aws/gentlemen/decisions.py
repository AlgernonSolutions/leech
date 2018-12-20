import json
import uuid

from toll_booth.alg_obj.aws.gentlemen.events import Event, EventHistory
from toll_booth.alg_obj.serializers import AlgEncoder


class DecisionParameters:
    def __init__(self, flow_type: str, task_token: str, flow_id: str, run_id: str, event_history: EventHistory):
        self._flow_type = flow_type
        self._task_token = task_token
        self._flow_id = flow_id
        self._run_id = run_id
        self._event_history = event_history

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
    def event_history(self):
        return self._event_history

    @property
    def task_token(self):
        return self._task_token

    @classmethod
    def parse_from_decision_poll(cls, poll_response):
        flow_type = poll_response['workflowType']['name']
        task_token = poll_response['taskToken']
        execution_info = poll_response['workflowExecution']
        flow_id = execution_info['workflowId']
        run_id = execution_info['runId']
        raw_events = poll_response['events']
        events = [Event.parse_from_decision_poll_event(x) for x in raw_events]
        event_history = EventHistory.generate_from_events(events)
        return cls(flow_type, task_token, flow_id, run_id, event_history)

    @classmethod
    def generate_empty(cls):
        return cls('', '', '', '', EventHistory())

    def add_additional_events(self, events):
        self._event_history.add_events(events)

    def __bool__(self):
        return self._flow_type is not ''


class Decision:
    def __init__(self, decision_type, decision_attributes, attributes_name):
        self._decision_type = decision_type
        self._decision_attributes = decision_attributes
        self._attributes_name = attributes_name

    def __getitem__(self, item):
        return self._decision_attributes[item]

    @property
    def decision_type(self):
        return self._decision_type

    @property
    def decision_attributes(self):
        return self._decision_attributes

    @property
    def attributes_name(self):
        return self._attributes_name


class ScheduleLambda(Decision):
    def __init__(self, function_name, task_args):
        execution_id = uuid.uuid4().hex
        lambda_attributes = {
            'id': execution_id,
            'name': function_name,
            'input': json.dumps(task_args, cls=AlgEncoder)
        }
        attributes_name = 'scheduleLambdaFunctionDecisionAttributes'
        super().__init__('ScheduleLambdaFunction', lambda_attributes, attributes_name)

    @property
    def id(self):
        return self.__getitem__('id')

    @property
    def name(self):
        return self.__getitem__('name')

    @property
    def input(self):
        return self.__getitem__('input')


class StartSubtask(Decision):
    def __init__(self, parent_id, subtask_type, task_args, lambda_role, **kwargs):
        subtask_attributes = {
            'workflowType': {
                'name': subtask_type,
                'version': kwargs.get('version', '1')
            },
            'workflowId': f'{subtask_type}-{parent_id}',
            'input': task_args,
            'taskList': {'name': kwargs.get('task_list_name', 'Leech')},
            'lambdaRole': lambda_role
        }
        attributes_name = 'startChildWorkflowExecutionDecisionAttributes'
        super().__init__('StartChildWorkflowExecution', subtask_attributes, attributes_name)

    @property
    def workflow_type(self):
        return self.__getitem__('workflowType')

    @property
    def workflow_id(self):
        return self.__getitem__('workflowId')

    @property
    def input(self):
        return self.__getitem__('input')

    @property
    def task_list(self):
        return self.__getitem__('taskList')


class CompleteWork(Decision):
    def __init__(self, results=None):
        complete_work_attributes = {
            'result': results
        }
        attributes_name = 'completeWorkflowExecutionDecisionAttributes'
        super().__init__('CompleteWorkflowExecution', complete_work_attributes, attributes_name)

    @property
    def results(self):
        return self.__getitem__('result')


class RecordMarker(Decision):
    def __init__(self, marker_name, details=None):
        marker_attributes = {
            'markerName': marker_name,
            'details': details
        }
        attributes_name = 'recordMarkerDecisionAttributes'
        super().__init__('RecordMarker', marker_attributes, attributes_name)

    @property
    def marker_name(self):
        return self.__getitem__('markerName')

    @property
    def details(self):
        return self.__getitem__('details')


class MadeDecisions:
    def __init__(self, task_token: str, decisions: [Decision] = None):
        if not decisions:
            decisions = []
        self._task_token = task_token
        self._decisions = decisions

    @property
    def for_commit(self):
        return {
            'taskToken': self._task_token,
            'decisions': [{
                'decisionType': x.decision_type,
                x.attributes_name: x.decision_attributes
            } for x in self._decisions]
        }

    def add_decision(self, decision: Decision):
        self._decisions.append(decision)


'ScheduleActivityTask'
'RequestCancelActivityTask'
'CompleteWorkflowExecution'
'FailWorkflowExecution'
'CancelWorkflowExecution'
'ContinueAsNewWorkflowExecution'
'RecordMarker'
'StartTimer'
'CancelTimer'
'SignalExternalWorkflowExecution'
'RequestCancelExternalWorkflowExecution'
'StartChildWorkflowExecution'
'ScheduleLambdaFunction'
