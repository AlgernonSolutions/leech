from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import ActivitySignature, group, SubtaskSignature, chain
from toll_booth.alg_tasks.rivers.rocks import workflow


@xray_recorder.capture('work_patient_id')
@workflow('work_patient_id')
def work_patient_id(**kwargs):
    execution_id = kwargs['execution_id']
    names = {
        'local': f'get_local_ids-{execution_id}',
        'remote': f'get_remote_ids-{execution_id}',
        'work_links': f'work_fip_links-{execution_id}',
        'change_types': f'pull_change_types-{execution_id}',
        'schema_entry': f'pull_schema_entry-{execution_id}',
        'map': f'build_mapping-{execution_id}'
    }
    kwargs['names'] = names
    decisions = kwargs['decisions']
    get_encounters_signature = _build_get_encounters(**kwargs)
    encounters_results = get_encounters_signature(**kwargs)
    if encounters_results is None:
        return
    great_group = _build_group(**kwargs)
    group_results = great_group(**kwargs)
    if group_results is None:
        return
    decisions.append(CompleteWork())


@xray_recorder.capture('work_patient_id_build_get_encounter_ids')
def _build_get_encounters(execution_id, **kwargs):
    activity_id = f'get_encounter_ids-{execution_id}'
    get_encounter_ids = ActivitySignature(activity_id, 'retrieve_client_encounters', **kwargs)
    return chain(get_encounter_ids)


@xray_recorder.capture('work_patient_id_build_group')
def _build_group(task_args, **kwargs):
    subtask_name = 'work_encounter_documentation'
    execution_id = kwargs['execution_id']
    encounter_ids = task_args.get_argument_value('encounter_ids')
    work_remote_ids_signatures = []
    for remote_id_value in encounter_ids:
        subtask_identifier = f'work_encounter_documentation-{remote_id_value}-{execution_id}'
        new_task_args = task_args.replace_argument_value(subtask_name, {'id_value': remote_id_value}, remote_id_value)
        work_remote_id_signature = SubtaskSignature(subtask_identifier, subtask_name, task_args=new_task_args, **kwargs)
        work_remote_ids_signatures.append(work_remote_id_signature)
    tuple_signatures = tuple(work_remote_ids_signatures)
    return group(*tuple_signatures)
