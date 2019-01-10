import json
import uuid

from toll_booth.alg_obj.aws.gentlemen.events.history import WorkflowHistory
from toll_booth.alg_obj.aws.gentlemen.events.subtasks import SubtaskOperation
from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder


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
    def __init__(self, subtask_identifier, subtask_type, flow_input, lambda_role, **kwargs):
        subtask_attributes = {
            'workflowType': {
                'name': subtask_type,
                'version': kwargs['version']
            },
            'workflowId': subtask_identifier,
            'input': flow_input,
            'taskList': {'name': subtask_type},
            'lambdaRole': lambda_role
        }
        attributes_name = 'startChildWorkflowExecutionDecisionAttributes'
        super().__init__('StartChildWorkflowExecution', subtask_attributes, attributes_name)

    @classmethod
    def for_retry(cls, failed_operation: SubtaskOperation):
        retry_args = {
            'parent_id': failed_operation.operation_id,
            'subtask_type': failed_operation.task_name,
            'task_args': failed_operation.task_args,
            'lambda_role': failed_operation.lambda_role,
            'task_list_name': failed_operation.task_list_name,
            'version': failed_operation.task_version
        }
        return cls(**retry_args)

    @property
    def workflow_type(self):
        return self.__getitem__('workflowType')

    @property
    def type_name(self):
        return self.workflow_type['name']

    @property
    def workflow_id(self):
        return self.__getitem__('workflowId')

    @property
    def input(self):
        return self.__getitem__('input')

    @property
    def task_list(self):
        return self.__getitem__('taskList')


class StartActivity(Decision):
    def __init__(self, activity_id, activity_name, input_string, **kwargs):
        activity_attributes = {
            'activityType': {
                'name': activity_name,
                'version': kwargs.get('version', '1')
            },
            'taskList': {'name': kwargs.get('task_list_name', 'Leech')},
            'activityId': activity_id,
            'input': input_string
        }
        attributes_name = 'scheduleActivityTaskDecisionAttributes'
        super().__init__('ScheduleActivityTask', activity_attributes, attributes_name)

    @classmethod
    def for_retry(cls, activity):
        activity_retry_args = {
            'activity_id': activity.activity_id,
            'activity_name': activity.activity_name,
            'input_string': activity.input_string,
            'version': activity.activity_version,
            'task_list_name': activity.task_list_name
        }
        return cls(**activity_retry_args)

    @property
    def activity_type(self):
        return self.__getitem__('activityType')

    @property
    def type_name(self):
        return self.activity_type['name']


class CompleteWork(Decision):
    def __init__(self, results=None):
        complete_work_attributes = {
            'result': results
        }
        if not results:
            complete_work_attributes = {}
        attributes_name = 'completeWorkflowExecutionDecisionAttributes'
        super().__init__('CompleteWorkflowExecution', complete_work_attributes, attributes_name)

    @property
    def results(self):
        return self.__getitem__('result')


class SignalExternalFlow(Decision):
    def __init__(self, flow_id, run_id, signal_name, signal_payload=None):
        signal_attributes = {
            'workflowId': flow_id,
            'runId': run_id,
            'signalName': signal_name
        }
        if signal_payload:
            signal_string = json.dumps(signal_payload, cls=AlgEncoder)
            signal_attributes['input'] = signal_string
        attributes_name = 'signalExternalWorkflowExecutionDecisionAttributes'
        super().__init__('SignalExternalWorkflowExecution', signal_attributes, attributes_name)

    @classmethod
    def for_subtask_completed(cls, work_history: WorkflowHistory):
        return cls(work_history.parent_flow_id, work_history.parent_run_id, 'subtask_completed')


class RecordMarker(Decision):
    def __init__(self, marker_name, details=None):
        marker_attributes = {
            'markerName': marker_name,
            'details': details
        }
        attributes_name = 'recordMarkerDecisionAttributes'
        super().__init__('RecordMarker', marker_attributes, attributes_name)

    @classmethod
    def for_names(cls, names):
        name_string = json.dumps(names, cls=AlgEncoder)
        return cls('names', name_string)

    @classmethod
    def for_checkpoint(cls, operation_identifier, operation_results):
        checkpoint_data = {operation_identifier: operation_results}
        checkpoint_string = json.dumps(checkpoint_data, cls=AlgEncoder)
        return cls('checkpoint', checkpoint_string)

    @property
    def marker_name(self):
        return self.__getitem__('markerName')

    @property
    def details(self):
        return self.__getitem__('details')


class StartTimer(Decision):
    def __init__(self, timer_name, duration_seconds, details=None):
        from datetime import datetime
        timestamp = str(datetime.utcnow().timestamp())
        details = json.dumps(details, cls=AlgEncoder)
        timer_attributes = {
            'timerId': f'{timer_name}!{timestamp}',
            'control': details,
            'startToFireTimeout': str(duration_seconds)
            }
        attributes_name = 'startTimerDecisionAttributes'
        super().__init__('StartTimer', timer_attributes, attributes_name)
        self._timer_name = timer_name
        self._duration_seconds = duration_seconds

    @property
    def timer_id(self):
        return self.__getitem__('timerId')

    @property
    def details(self):
        return json.loads(self.__getitem__('controls'), cls=AlgDecoder)

    @property
    def duration_seconds(self):
        return self._duration_seconds


class ContinueAsNew(Decision):
    def __init__(self, task_args, lambda_role, **kwargs):
        continuation_attributes = {
            'lambdaRole': lambda_role,
            'input': task_args,
            'taskList': {'name': kwargs.get('task_list_name', 'Leech')}
        }
        attributes_name = 'continueAsNewWorkflowExecutionDecisionAttributes'
        super().__init__('ContinueAsNewWorkflowExecution', continuation_attributes, attributes_name)


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
