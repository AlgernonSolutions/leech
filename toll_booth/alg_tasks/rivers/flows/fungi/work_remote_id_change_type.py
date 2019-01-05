import json

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import Signature, group, chain
from toll_booth.alg_obj.serializers import AlgDecoder
from toll_booth.alg_tasks.rivers.rocks import workflow

"""
    this flow processes a single change_type for a single remote_id value,
    it will further divide into flows each unique change_action under the change_type    
"""


@workflow
def work_remote_id_change_type(**kwargs):
    decisions = kwargs['decisions']
    execution_id = kwargs['execution_id']
    task_args = kwargs['task_args']
    source_kwargs = task_args['source']
    category_id = source_kwargs['category_id']
    id_value = source_kwargs['id_value']
    changelog_types = source_kwargs['changelog_types']
    change_category = changelog_types.categories[category_id]
    names = {
        'local_max': f'get_local_max-{change_category}-{id_value}-{execution_id}',
        'work': f'work_remote_id_change_type-{change_category}-{id_value}-{execution_id}'
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


def _build_local_max_signature(**kwargs):
    names = kwargs['names']
    fn_identifier = names['local_max']
    get_local_max_signature = Signature.for_activity(fn_identifier, 'get_local_max_change_type_value', **kwargs)
    return get_local_max_signature


def _build_work_signature(**kwargs):
    names = kwargs['names']
    fn_identifier = names['work']
    work_signature = Signature.for_activity(fn_identifier, 'work_remote_id_change_type', **kwargs)
    return work_signature


def _build_group(**kwargs):
    signatures = []
    execution_id = kwargs['execution_id']
    task_args = kwargs['task_args']
    activities = kwargs['activities']
    names = kwargs['names']
    source_kwargs = task_args['source']
    category_id = source_kwargs['category_id']
    id_value = source_kwargs['id_value']
    changelog_types = source_kwargs['changelog_types']
    local_max_operation = activities[names['local_max']]
    work_operation = activities[names['work']]
    max_local_value = json.loads(local_max_operation.results, cls=AlgDecoder)
    remote_changes = json.loads(work_operation.results, cls=AlgDecoder)
    change_category = changelog_types.categories[category_id]
    for action_id, change_action in change_category.change_types.items():
        task_args.add_arguments({'source': max_local_value})
        task_args.add_arguments({'source': remote_changes})
        task_args.add_arguments({'source': {'action_id': action_id}})
        subtask_identifier = f'work_remote_id_change_action-{change_action}-{change_category}-{id_value}-{execution_id}'
        change_type_signature = Signature.for_subtask(subtask_identifier, 'work_remote_id_change_action', **kwargs)
        signatures.append(change_type_signature)
    tuple_signatures = tuple(signatures)
    return group(*tuple_signatures)
