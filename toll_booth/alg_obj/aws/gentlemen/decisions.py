import json
import uuid

from toll_booth.alg_obj.serializers import AlgEncoder


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
