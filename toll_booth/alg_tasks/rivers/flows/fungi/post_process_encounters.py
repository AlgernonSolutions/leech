from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import SubtaskSignature, ActivitySignature, chain
from toll_booth.alg_tasks.rivers.rocks import workflow


@xray_recorder.capture('post_process_encounters')
@workflow('post_process_encounters')
def post_process_encounters(**kwargs):
    decisions = kwargs['decisions']
    get_encounters_signature = _build_get_encounters_signature(**kwargs)
    parse_encounters_signature = _build_parse_encounters_signature(**kwargs)
    process_chain = chain(get_encounters_signature, parse_encounters_signature)
    chain_results = process_chain(**kwargs)
    if chain_results is None:
        return
    fungal_signature = _build_fungal_leech(**kwargs)
    fungal_results = fungal_signature(**kwargs)
    if fungal_results is None:
        return
    decisions.append(CompleteWork())


def _build_get_encounters_signature(execution_id, **kwargs):
    fn_name = 'post_process_get_encounters'
    fn_identifier = f'{fn_name}-{execution_id}'
    return ActivitySignature(fn_identifier, fn_name, **kwargs)


def _build_parse_encounters_signature(execution_id, **kwargs):
    fn_name = 'post_process_parse_encounters'
    fn_identifier = f'{fn_name}-{execution_id}'
    return ActivitySignature(fn_identifier, fn_name, **kwargs)


def _build_fungal_leech(execution_id, task_args, **kwargs):
    subtask_name = 'fungal_leech'
    subtask_identifier = f'fungal_leech-{execution_id}'
    extracted_data = task_args.get_argument_value('data_fields')
    new_task_args = task_args.replace_argument_value(subtask_name, {'extracted_data': extracted_data})
    fungal_leech = SubtaskSignature(subtask_identifier, subtask_name, task_args=new_task_args, **kwargs)
    return fungal_leech
