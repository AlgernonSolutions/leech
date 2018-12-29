from toll_booth.alg_tasks.rivers.rocks import workflow
from toll_booth.alg_obj.aws.gentlemen.decisions import StartSubtask, CompleteWork
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder
import json


@workflow
def work_remote_id(decisions, **kwargs):
    working = False
    change_category_operation_names = _start_change_categories(**kwargs)
    if change_category_operation_names is None:
        return
    for operation_name in change_category_operation_names:
        working = _check_change_category_operation(operation_name, **kwargs)
    if not working:
        decisions.append(CompleteWork())


def _start_change_categories(execution_id, input_kwargs, sub_tasks, names, decisions, activities, **kwargs):
    working = False
    category_operation_names = []
    get_change_type_operation = activities[names['change_types']]
    change_types = json.loads(get_change_type_operation.results, cls=AlgDecoder)
    id_value = input_kwargs['id_value']
    for category_id, change_category in change_types.categories.items():
        input_kwargs['category_id'] = str(category_id)
        input_kwargs['category'] = change_category
        input_kwargs['change_types'] = get_change_type_operation.results
        remote_category_name = f'work_remote_id_change_type-{change_category}-{id_value}-{execution_id}'
        remote_category_name_base = f'work_remote_id_change_type-{change_category}-{id_value}'
        if remote_category_name not in sub_tasks:
            flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
            decisions.append(StartSubtask(
                execution_id, 'work_remote_id_change_type', flow_input, version='1',
                task_list_name='Leech', lambda_role=kwargs.get('lambda_role'), name_base=remote_category_name_base
            ))
            working = True
        category_operation_names.append(remote_category_name)
    if working:
        return None
    return category_operation_names


def _check_change_category_operation(change_category_operation_name, sub_tasks, **kwargs):
    change_category_operation = sub_tasks[change_category_operation_name]
    if change_category_operation.is_complete:
        return False
    if change_category_operation.is_failed:
        fail_count = sub_tasks.get_operation_failed_count(change_category_operation.flow_id)
        if fail_count >= 3:
            raise RuntimeError('failure in an subtask: %s' % change_category_operation_name)
        decisions = kwargs['decisions']
        decisions.append(StartSubtask.for_retry(change_category_operation))
    return True
