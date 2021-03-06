import json
import os

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


class StartLambda(Decision):
    def __init__(self, lambda_id, function_name, task_args=None, **kwargs):
        is_vpc = kwargs['is_vpc']
        running_time = kwargs.get('running_time', 300)
        lambda_fn_name = os.getenv('LABOR_FUNCTION', 'leech-lambda-labor')
        if is_vpc:
            lambda_fn_name = os.getenv('VPC_LABOR_FUNCTION', 'leech-vpc-labor')
        lambda_attributes = {
            'id': lambda_id,
            'name': lambda_fn_name,
            'startToCloseTimeout': str(running_time)
        }
        if task_args:
            lambda_attributes['input'] = json.dumps({
                'task_name': function_name,
                'task_args': task_args,
                'flow_id': kwargs.get('flow_id', '0'), 'run_id': kwargs.get('run_id', '0'),
                'task_id': lambda_id,
                'register_results': False
            }, cls=AlgEncoder)
        if kwargs.get('control', None) is not None:
            lambda_attributes['control'] = kwargs['control']
        attributes_name = 'scheduleLambdaFunctionDecisionAttributes'
        super().__init__('ScheduleLambdaFunction', lambda_attributes, attributes_name)
        self._fn_name = function_name

    @property
    def id(self):
        return self.__getitem__('id')

    @property
    def name(self):
        return self.__getitem__('name')

    @property
    def type_name(self):
        return self._fn_name

    @property
    def input_args(self):
        return self.__getitem__('input')

    def set_id_info(self, flow_id, run_id):
        input_string = self._decision_attributes['input']
        input_data = json.loads(input_string, cls=AlgDecoder)
        input_data['flow_id'] = flow_id
        input_data['run_id'] = run_id
        self._decision_attributes['input'] = json.dumps(input_data, cls=AlgEncoder)


class StartSubtask(Decision):
    def __init__(self, subtask_identifier, subtask_type, task_args, lambda_role, **kwargs):
        subtask_attributes = {
            'workflowType': {
                'name': subtask_type,
                'version': kwargs['version']
            },
            'workflowId': subtask_identifier,
            'input': json.dumps({'task_args': task_args}, cls=AlgEncoder),
            'taskList': {'name': kwargs.get('task_list', subtask_identifier)},
            'lambdaRole': lambda_role
        }
        if 'control' in kwargs:
            subtask_attributes['control'] = kwargs['control']
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

    @property
    def control(self):
        return self.__getitem__('control')

    def set_control(self, control_string):
        self._decision_attributes['control'] = control_string

    def set_parent_data(self, config, versions):
        input_string = self._decision_attributes['input']
        input_data = json.loads(input_string, cls=AlgDecoder)
        input_data.update({'config': config, 'versions': versions})
        self._decision_attributes['input'] = json.dumps(input_data, cls=AlgEncoder)


class StartActivity(Decision):
    def __init__(self, activity_id, activity_name, task_args=None, **kwargs):
        running_time = kwargs.get('running_time', 300)
        heartbeat = kwargs.get('heartbeat', 'NONE')
        activity_attributes = {
            'activityType': {
                'name': activity_name,
                'version': kwargs.get('version', '1')
            },
            'taskList': {'name': kwargs.get('task_list', 'Leech')},
            'activityId': activity_id,
            'startToCloseTimeout': str(running_time),
            'heartbeatTimeout': str(heartbeat)
        }
        if task_args:
            activity_attributes['input'] = json.dumps({
                'task_name': activity_name,
                'task_args': task_args,
                'flow_id': kwargs.get('flow_id', '0'), 'run_id': kwargs.get('run_id', '0'),
                'task_id': activity_id,
                'register_results': kwargs.get('register_results', True)
            }, cls=AlgEncoder)
        if 'control' in kwargs:
            activity_attributes['control'] = json.dumps(kwargs['control'], cls=AlgEncoder)
        attributes_name = 'scheduleActivityTaskDecisionAttributes'
        super().__init__('ScheduleActivityTask', activity_attributes, attributes_name)

    @classmethod
    def for_retry(cls, activity):
        activity_retry_args = {
            'activity_id': activity.activity_id,
            'activity_name': activity.activity_name,
            'input_string': activity.input_string,
            'version': activity.activity_version,
            'task_list': activity.task_list_name
        }
        return cls(**activity_retry_args)

    @property
    def activity_type(self):
        return self.__getitem__('activityType')

    @property
    def type_name(self):
        return self.activity_type['name']

    @property
    def control(self):
        return self.__getitem__('control')

    def set_control(self, control_string):
        self._decision_attributes['control'] = control_string

    def set_id_info(self, flow_id, run_id):
        input_string = self._decision_attributes['input']
        input_data = json.loads(input_string, cls=AlgDecoder)
        input_data['flow_id'] = flow_id
        input_data['run_id'] = run_id
        self._decision_attributes['input'] = json.dumps(input_data, cls=AlgEncoder)


class CompleteWork(Decision):
    def __init__(self, results=None):
        complete_work_attributes = {}
        if results:
            results = json.dumps(results, cls=AlgEncoder)
            complete_work_attributes = {
                'result': results
            }
        attributes_name = 'completeWorkflowExecutionDecisionAttributes'
        super().__init__('CompleteWorkflowExecution', complete_work_attributes, attributes_name)

    @property
    def results(self):
        return self.__getitem__('result')

    def add_result(self, result_name, results):
        current_results = self._decision_attributes.get('result', {})
        if current_results:
            current_results = json.loads(current_results, cls=AlgDecoder)
        if result_name in current_results:
            raise RuntimeError(f'tried to store results: {results}, named: {result_name} in current results: {current_results},'
                               f' overwriting in the complete work decision is not supported')
        current_results[result_name] = results
        self._decision_attributes['result'] = json.dumps(current_results, cls=AlgEncoder)


class CancelWork(Decision):
    def __init__(self, cancel_details=None):
        complete_work_attributes = {}
        if cancel_details:
            results = json.dumps(cancel_details, cls=AlgEncoder)
            complete_work_attributes = {
                'details': results
            }
        attributes_name = 'cancelWorkflowExecutionDecisionAttributes'
        super().__init__('CancelWorkflowExecution', complete_work_attributes, attributes_name)

    @property
    def cancel_details(self):
        return self.__getitem__('details')


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

    @classmethod
    def for_versions(cls, versions):
        version_data = json.dumps(versions, cls=AlgEncoder)
        return cls('versions', version_data)

    @classmethod
    def for_config(cls, config):
        config_data = json.dumps(config, cls=AlgEncoder)
        return cls('config', config_data)

    @classmethod
    def for_ruffian(cls, task_identifier, execution_arn, is_close=False):
        ruffian_data = {'task_identifier': task_identifier, 'execution_arn': execution_arn, 'is_close': is_close}
        data_string = json.dumps(ruffian_data, cls=AlgEncoder)
        return cls('ruffian', data_string)

    @classmethod
    def for_signal_completed(cls, signal_id, signal_name):
        signal_data = {'signal_id': signal_id, 'signal_name': signal_name}
        data_string = json.dumps(signal_data, cls=AlgEncoder)
        return cls('signal', data_string)

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


class CancelTask(Decision):
    def __init__(self, **kwargs):
        cancel_attributes = {
            'activityId': kwargs['activity_id']
        }
        attributes_name = 'requestCancelActivityTaskDecisionAttributes'
        super().__init__('RequestCancelActivityTask', cancel_attributes, attributes_name)


class MadeDecisions:
    def __init__(self, task_token: str, decisions: [Decision] = None):
        if not decisions:
            decisions = []
        self._task_token = task_token
        self._decisions = decisions

    @property
    def decisions(self):
        return self._decisions

    @property
    def for_commit(self):
        if not self.check_decision_order():
            self.reorder_decisions()
        return {
            'taskToken': self._task_token,
            'decisions': [{
                'decisionType': x.decision_type,
                x.attributes_name: x.decision_attributes
            } for x in self._decisions]
        }

    def check_decision_order(self):
        close_decisions = [x for x in self._decisions if isinstance(x, CompleteWork)]
        if not close_decisions:
            return True
        for decision in close_decisions:
            if decision is self._decisions[-1]:
                return True
            return False

    def add_decision(self, decision: Decision):
        self._decisions.append(decision)

    def reorder_decisions(self):
        close_decisions = [x for x in self._decisions if isinstance(x, CompleteWork)]
        other_decisions = [x for x in self._decisions if not isinstance(x, CompleteWork)]
        other_decisions.extend(close_decisions)
        self._decisions = other_decisions
        if not self.check_decision_order():
            raise RuntimeError(f'even after reordering, decisions incorrectly ordered: {self._decisions}')
