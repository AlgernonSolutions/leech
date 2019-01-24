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
from toll_booth.alg_obj.aws.gentlemen.rafts import chain, ActivitySignature, LambdaSignature, SubtaskSignature, group
from toll_booth.alg_tasks.rivers.rocks import workflow


@xray_recorder.capture('work_remote_id_change_action')
@workflow('work_remote_id_change_action')
def work_remote_id_change_action(**kwargs):
    decisions = kwargs['decisions']
    execution_id = kwargs['execution_id']
    names = {
        'enrich': f'enrichment-{execution_id}',
        'change': f'generate-{execution_id}'
    }
    kwargs['names'] = names
    enrich_signature = _build_enrich_signature(**kwargs)
    change_data_signatures = _build_change_data_signatures(**kwargs)
    if change_data_signatures is None:
        decisions.append(CompleteWork())
        return
    great_chain = chain(enrich_signature, group(*tuple(change_data_signatures)))
    chain_results = great_chain(**kwargs)
    if chain_results is None:
        return
    fungal_leech = _build_fungal_leech(**kwargs)
    leech_results = fungal_leech(**kwargs)
    if leech_results is None:
        return
    decisions.append(CompleteWork())


@xray_recorder.capture('work_remote_id_change_action_build_enrich_signature')
def _build_enrich_signature(**kwargs):
    names = kwargs['names']
    fn_identifier = names['enrich']
    enrich_signature = ActivitySignature(fn_identifier, 'get_enrichment_for_change_action', **kwargs)
    return enrich_signature


@xray_recorder.capture('work_remote_id_change_action_build_change_data_signatures')
def _build_change_data_signatures(task_args, **kwargs):
    import json
    from toll_booth.alg_obj.serializers import AlgEncoder
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
        remote_change_identifier = json.dumps(remote_change, cls=AlgEncoder)
        new_task_args = task_args.replace_argument_value(subtask_name, {'remote_change': remote_change}, remote_change_identifier)
        signature = LambdaSignature(fn_identifier, subtask_name, task_args=new_task_args, **kwargs)
        signatures.append(signature)
    if not signatures:
        return None
    return signatures


@xray_recorder.capture('work_remote_id_change_action_build_fungal_leech')
def _build_fungal_leech(execution_id, task_args, **kwargs):
    subtask_name = 'fungal_leech'
    subtask_identifier = f'fungal_leech-{execution_id}'
    extracted_data = task_args.get_argument_value('change_data')
    new_task_args = task_args.replace_argument_value(subtask_name, {'extracted_data': extracted_data})
    fungal_leech = SubtaskSignature(subtask_identifier, subtask_name, task_args=new_task_args, **kwargs)
    return fungal_leech
