from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import Signature, group, SubtaskSignature
from toll_booth.alg_tasks.rivers.rocks import workflow


@xray_recorder.capture('work_remote_id')
@workflow('work_remote_id')
def work_remote_id(task_args, **kwargs):
    subtask_name = 'work_remote_id_change_type'
    decisions = kwargs['decisions']
    execution_id = kwargs['execution_id']
    changelog_types = task_args.get_argument_value('changelog_types')
    signatures = []
    for category_id, change_category in changelog_types.categories.items():
        new_task_args = task_args.replace_argument_value(subtask_name, {'category_id': category_id}, category_id)
        subtask_identifier = f'work_type-{change_category}-{execution_id}'
        category_signature = SubtaskSignature(subtask_identifier, subtask_name, task_args=new_task_args, **kwargs)
        signatures.append(category_signature)
    tuple_signatures = tuple(signatures)
    great_group = group(*tuple_signatures)
    group_results = great_group(**kwargs)
    if group_results is None:
        return
    decisions.append(CompleteWork())
