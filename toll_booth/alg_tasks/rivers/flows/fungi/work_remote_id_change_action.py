"""
    This flow is the terminal flow for the unholy FungalLeech process,
    by this point, we have extracted all the driving vertexes, and then propagated out the change_categories,
    followed by the change_types, then ultimately down to the actual change itself.
    The change itself may need additional information, called enrichment, which was not produced through the initial
    extraction. If so, that data is gathered, and finally the entire thing is collected together into a single package,
    which we can then transform, assimilate, etc.
"""

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import Signature, chain
from toll_booth.alg_tasks.rivers.rocks import workflow


@workflow('work_remote_id_change_action')
def work_remote_id_change_action(**kwargs):
    decisions = kwargs['decisions']
    execution_id = kwargs['execution_id']
    task_args = kwargs['task_args']
    source_kwargs = task_args['source']
    category_id = source_kwargs['category_id']
    id_value = source_kwargs['id_value']
    changelog_types = source_kwargs['changelog_types']
    change_category = changelog_types.categories[category_id]
    action_id = source_kwargs['action_id']
    change_action = change_category[action_id]
    names = {
        'enrich': f'get_enrichment_for_change_action-{change_action}-{change_category}-{id_value}-{execution_id}',
        'change': f'generate_remote_id_change_data-{change_action}-{change_category}-{id_value}-{execution_id}',
    }
    kwargs['names'].update(names)
    enrich_signature = _build_enrich_signature(**kwargs)
    change_data_group = _build_change_data_group(**kwargs)
    great_chain = chain(enrich_signature, change_data_group)
    chain_results = great_chain(**kwargs)
    if chain_results is None:
        return
    decisions.append(CompleteWork(chain_results))


def _build_enrich_signature(**kwargs):
    names = kwargs['names']
    fn_identifier = names['enrich']
    enrich_signature = Signature.for_activity(fn_identifier, 'get_enrichment_for_change_action', **kwargs)
    return enrich_signature


def _build_change_data_group(**kwargs):
    subtask_name = 'generate_remote_id_change_data'
    signatures = []
    names = kwargs['names']
    activities = kwargs['activities']
    fn_identifier = names['change']
    task_args = kwargs['task_args']
    source_args = task_args['source']
    change_actions = activities.get_result_value(kwargs['names']['change_types'])
    action_id = source_args['action_id']
    changelog_types = source_args['changelog_types']
    change_action = changelog_types[action_id]
    remote_actions = change_actions[change_action.action_name]
    for remote_change in remote_actions:
        task_args.add_argument_value({subtask_name: {'remote_change': remote_change}})
        signature = Signature.for_activity(fn_identifier, subtask_name, **kwargs)
        signatures.append(signature)
    return signatures
