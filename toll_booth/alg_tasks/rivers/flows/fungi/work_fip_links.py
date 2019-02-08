from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import LambdaSignature, ActivitySignature, group
from toll_booth.alg_tasks.rivers.rocks import workflow


# @xray_recorder.capture('work_fip_links')
@workflow('work_fip_links')
def work_fip_links(**kwargs):
    decisions = kwargs['decisions']
    execution_id = kwargs['execution_id']
    names = {
        'unlink_old_id': f'unlink_old_id-{execution_id}',
        'link_new_id': f'link_new_id-{execution_id}',
        'put_new_id': f'put_new_id-{execution_id}',
        'graph': f'graph_fip_links-{execution_id}'
    }
    kwargs['names'] = names
    index_group = _build_index_group(**kwargs)
    index_results = index_group(**kwargs)
    if index_results is None:
        return
    decisions.append(CompleteWork())


def _build_index_group(task_args, **kwargs):
    signatures = []
    names = kwargs['names']
    local_id_values = task_args.get_argument_value('local_id_values')
    remote_id_values = task_args.get_argument_value('remote_id_values')
    local_linked_values = local_id_values['linked']
    operations = [
        ('unlink_old_id', local_linked_values - remote_id_values),
        ('link_new_id', remote_id_values - local_linked_values),
        ('put_new_id', remote_id_values - local_id_values['all'])
    ]
    for operation in operations:
        operation_name = operation[0]
        id_values = operation[1]
        operation_identifier = names[operation_name]
        new_task_arg = {'id_values': id_values}
        new_task_args = task_args.replace_argument_value(operation_name, new_task_arg, operation_identifier)
        operation_signature = LambdaSignature(operation_identifier, operation_name, task_args=new_task_args, **kwargs)
        signatures.append(operation_signature)
    if not signatures:
        return None
    return group(*tuple(signatures))
