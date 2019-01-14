"""
    This flow is the terminal flow for the unholy FungalLeech process,
    by this point, we have extracted all the driving vertexes, and then propagated out the change_categories,
    followed by the change_types, then ultimately down to the actual change itself.
    The change itself may need additional information, called enrichment, which was not produced through the initial
    extraction. If so, that data is gathered, and finally the entire thing is collected together into a single package,
    which we can then transform, assimilate, etc.
"""
from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import Signature, chain
from toll_booth.alg_tasks.rivers.rocks import workflow


@xray_recorder.capture('work_remote_id_change_action')
@workflow('work_remote_id_change_action')
def work_remote_id_change_action(**kwargs):
    decisions = kwargs['decisions']
    execution_id = kwargs['execution_id']
    names = {
        'enrich': f'enrichment-{execution_id}',
        'change': f'generate-{execution_id}',
    }
    kwargs['names'] = names
    enrich_signature = _build_enrich_signature(**kwargs)
    change_data_group = _build_change_data_group(**kwargs)
    if change_data_group is None:
        decisions.append(CompleteWork())
        return
    great_chain = chain(enrich_signature, change_data_group)
    chain_results = great_chain(**kwargs)
    if chain_results is None:
        return
    decisions.append(CompleteWork(chain_results))


@xray_recorder.capture('work_remote_id_change_action_build_enrich_signature')
def _build_enrich_signature(**kwargs):
    names = kwargs['names']
    fn_identifier = names['enrich']
    enrich_signature = Signature.for_activity(fn_identifier, 'get_enrichment_for_change_action', **kwargs)
    return enrich_signature


@xray_recorder.capture('work_remote_id_change_action_build_change_data_group')
def _build_change_data_group(task_args, **kwargs):
    subtask_name = 'generate_remote_id_change_data'
    signatures = []
    names = kwargs['names']
    workflow_args = kwargs['workflow_args']
    fn_identifier = names['change']
    change_actions = task_args.get_argument_value('change_actions')
    changelog_types = task_args.get_argument_value('changelog_types')
    action_id = workflow_args['action_id']
    change_action = changelog_types[str(action_id)]
    remote_actions = change_actions.get(change_action.action, {})
    for remote_change in remote_actions:
        new_task_args = task_args.replace_argument_value(subtask_name, {'remote_change': remote_change}, remote_change)
        signature = Signature.for_activity(fn_identifier, subtask_name, new_task_args, **kwargs)
        signatures.append(signature)
    if not signatures:
        return None
    return signatures
