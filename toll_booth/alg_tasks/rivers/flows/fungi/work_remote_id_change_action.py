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
from toll_booth.alg_obj.aws.gentlemen.rafts import chain, ActivitySignature, SubtaskSignature, group
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
    fungal_leech(**kwargs)
    if not fungal_leech.is_complete:
        return
    post_process_group = _build_post_process_group(**kwargs)
    post_process_results = post_process_group(**kwargs)
    if post_process_results is None:
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
    batch_size = 100
    execution_id = kwargs['execution_id']
    subtask_name = 'batch_generate_remote_id_change_data'
    signatures = []
    names = kwargs['names']
    workflow_args = kwargs['workflow_args']
    fn_identifier = names['change']
    change_actions = task_args.get_argument_value('change_actions')
    if isinstance(change_actions, list):
        change_actions = change_actions[0]
    changelog_types = task_args.get_argument_value('changelog_types')
    action_id = workflow_args['action_id']
    change_action = changelog_types[str(action_id)]
    remote_actions = change_actions.get(change_action.action, {})
    batches = []
    batch = []
    for remote_change in remote_actions:
        if len(batch) > batch_size:
            batches.append(batch)
            batch = []
        batch.append(remote_change)
    if batch:
        batches.append(batch)
    for batch in batches:
        batch_identifier = f'{batches.index(batch)}-{subtask_name}-{execution_id}'
        new_task_args = task_args.replace_argument_value(subtask_name, {'remote_changes': batch}, batch_identifier)
        lambda_identifier = f'{batch_identifier}-{fn_identifier}'
        signature = ActivitySignature(lambda_identifier, subtask_name, task_args=new_task_args, **kwargs)
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


@xray_recorder.capture('work_remote_id_change_action_build_post_process')
def _build_post_process_group(execution_id, task_args, **kwargs):
    ext_id_values = task_args.get_argument_value('ext_id_values')

    signatures = []
    encounter_ids = []
    for entry in ext_id_values:
        id_type = entry[1]
        id_value = entry[3]
        if id_type == 'ClientVisit':
            encounter_ids.append(id_value)
    if encounter_ids:
        subtask_name = 'post_process_encounters'
        subtask_identifier = f'post_process_encounters-{execution_id}'
        new_task_args = task_args.replace_argument_value(subtask_name, {'encounter_ids': encounter_ids}, subtask_identifier)
        signatures.append(SubtaskSignature(subtask_identifier, subtask_name, task_args=new_task_args))
    return group(*tuple(signatures))
