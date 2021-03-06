from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import ActivitySignature, SubtaskSignature, group, chain, LambdaSignature
from toll_booth.alg_tasks.rivers.rocks import workflow

"""
    this flow processes a single change_type for a single remote_id value,
    it will further divide into flows each unique change_action under the change_type    
"""


@xray_recorder.capture('work_remote_id_change_type')
@workflow('work_remote_id_change_type')
def work_remote_id_change_type(**kwargs):
    decisions = kwargs['decisions']
    execution_id = kwargs['execution_id']
    names = {
        'local_max': f'get_local_max-{execution_id}',
        'work': f'work_remote_id_change_type-{execution_id}'
    }
    kwargs['names'] = names
    get_local_max_signature = _build_local_max_signature(**kwargs)
    work_signature = _build_work_signature(**kwargs)
    great_chain = chain(get_local_max_signature, work_signature)
    chain_results = great_chain(**kwargs)
    if chain_results is None:
        return
    great_group = _build_group(**kwargs)
    group_results = great_group(**kwargs)
    if group_results is None:
        return
    decisions.append(CompleteWork())


@xray_recorder.capture('work_remote_id_change_type_build_local_max')
def _build_local_max_signature(**kwargs):
    names = kwargs['names']
    fn_identifier = names['local_max']
    get_local_max_signature = ActivitySignature(fn_identifier, 'get_local_max_change_type_value', **kwargs)
    return get_local_max_signature


@xray_recorder.capture('work_remote_id_build_work_signature')
def _build_work_signature(**kwargs):
    names = kwargs['names']
    fn_identifier = names['work']
    work_signature = ActivitySignature(fn_identifier, 'work_remote_id_change_type', **kwargs)
    return work_signature


@xray_recorder.capture('work_remote_id_build_group')
def _build_group(task_args, **kwargs):
    subtask_name = 'work_remote_id_change_action'
    signatures = []
    execution_id = kwargs['execution_id']
    workflow_args = kwargs['workflow_args']
    category_id = workflow_args['category_id']
    changelog_types = task_args.get_argument_value('changelog_types')
    change_category = changelog_types.categories[category_id]
    change_actions = task_args.get_argument_value('change_actions')
    if isinstance(change_actions, list):
        change_actions = change_actions[0]
    for action_id, change_action in change_category.change_types.items():
        change_action = changelog_types[str(action_id)]
        remote_actions = change_actions.get(change_action.action, {})
        if not remote_actions:
            continue
        new_task_args = task_args.replace_argument_value(subtask_name, {'action_id': action_id}, action_id)
        subtask_identifier = f'work_action-{change_action}-{execution_id}'
        change_type_signature = SubtaskSignature(subtask_identifier, subtask_name, task_args=new_task_args, **kwargs)
        signatures.append(change_type_signature)
    tuple_signatures = tuple(signatures)
    return group(*tuple_signatures)
