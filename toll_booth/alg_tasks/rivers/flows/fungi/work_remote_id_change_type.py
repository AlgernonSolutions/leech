import json

from toll_booth.alg_obj.aws.gentlemen.decisions import StartActivity, StartSubtask, CompleteWork
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder
from toll_booth.alg_tasks.rivers.rocks import workflow
from toll_booth.alg_obj.aws.snakes.snakes import StoredData

"""
    this flow processes a single change_type for a single remote_id value,
    it will further divide into flows each unique change_action under the change_type    
"""


@workflow
def work_remote_id_change_type(decisions, **kwargs):
    working = False

    local_max_value = _get_local_max_value(**kwargs)
    if local_max_value is None:
        return
    change_actions = _get_remote_change_type(local_max_value, **kwargs)
    if change_actions is None:
        return
    operation_names = _start_change_actions(change_actions, **kwargs)
    for operation_name in operation_names:
        working = _check_operation(operation_name, **kwargs)
    if not working:
        decisions.append(CompleteWork())


def _get_local_max_value(execution_id, decisions, activities, input_kwargs, **kwargs):
    id_value = input_kwargs['id_value']
    change_category = input_kwargs['category']
    local_max_name = f'get_local_max_change_type_value-{change_category}-{id_value}-{execution_id}'
    if local_max_name not in activities:
        flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
        decisions.append(
            StartActivity(local_max_name, 'get_local_max_change_type_value', flow_input, task_list_name='Credible'))
        return None
    local_max_operation = activities[local_max_name]
    if not local_max_operation.is_complete:
        return None
    return json.loads(local_max_operation.results, cls=AlgDecoder)


def _get_remote_change_type(local_max_value, activities, decisions, execution_id, input_kwargs, **kwargs):
    input_kwargs['local_max_value'] = local_max_value
    id_value = input_kwargs['id_value']
    change_category = input_kwargs['category']
    activity_name = f'work_remote_id_change_type-{change_category}-{id_value}-{execution_id}'
    if activity_name not in activities:
        flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
        decisions.append(
            StartActivity(activity_name, 'work_remote_id_change_type', flow_input, task_list_name='Credible'))
        return None
    activity = activities[activity_name]
    if not activity.is_complete:
        return None
    if activity.is_failed:
        fail_count = activities.get_activity_failed_count(activity.activity_id)
        if fail_count >= 3:
            raise RuntimeError('failure in an activity: %s' % activity_name)
        decisions.append(StartActivity.for_retry(activity))
        return None
    return json.loads(activity.results, cls=AlgDecoder)


def _start_change_actions(change_actions, execution_id, sub_tasks, input_kwargs, decisions, **kwargs):
    working = False
    operation_names = []
    category = input_kwargs['category']
    id_value = input_kwargs['id_value']
    for action_name, change_action_set in change_actions:
        input_kwargs['action_name'] = action_name
        input_kwargs['category_id'] = str(category.category_id)
        input_kwargs['category'] = category
        input_kwargs['change_actions'] = StoredData.from_object('change_action_set', change_action_set, full_unpack=True)
        operation_name = f'work_remote_id_change_action-{action_name}-{category}-{id_value}-{execution_id}'
        remote_category_name_base = f'work_remote_id_change_action-{action_name}-{category}-{id_value}'
        if operation_name not in sub_tasks:
            flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
            decisions.append(StartSubtask(
                execution_id, 'work_remote_id_change_action', flow_input, version='1',
                task_list_name='Leech', lambda_role=kwargs.get('lambda_role'), name_base=remote_category_name_base
            ))
            working = True
        operation_names.append(operation_name)
    if working:
        return None
    return operation_names


def _check_operation(operation_name, sub_tasks, **kwargs):
    operation = sub_tasks[operation_name]
    if operation.is_complete:
        return False
    if operation.is_failed:
        fail_count = sub_tasks.get_operation_failed_count(operation.flow_id)
        if fail_count >= 3:
            raise RuntimeError('failure in an subtask: %s' % operation)
        decisions = kwargs['decisions']
        decisions.append(StartSubtask.for_retry(operation))
    return True