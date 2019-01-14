"""
    the master flow for the weird bit of getting information out of a FIP ICAMS domain,
    this flow works by vertex driven growth,
    under this process, we are unable to access the unique identifiers of the object of interest,
    so we instead access them through an object we can get unique identifiers for, creating a two step process
"""
from copy import deepcopy

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import Signature, group, chain
from toll_booth.alg_obj.aws.gentlemen.tasks import TaskArguments
from toll_booth.alg_tasks.rivers.rocks import workflow


# @xray_recorder.capture('command_fungi')
@workflow('command_fungi')
def command_fungi(**kwargs):
    execution_id = kwargs['execution_id']
    names = {
        'local': f'get_local_ids-{execution_id}',
        'remote': f'get_remote_ids-{execution_id}',
        'put': f'put_new_ids-{execution_id}',
        'link': f'link_new_ids-{execution_id}',
        'unlink': f'unlink_old_ids-{execution_id}',
        'change_types': f'pull_change_types-{execution_id}',
        'map': f'build_mapping-{execution_id}'
    }
    kwargs['names'] = names
    decisions = kwargs['decisions']
    great_chain = _build_chain(**kwargs)
    chain_results = great_chain(**kwargs)
    if chain_results is None:
        return
    great_group = _build_group(**kwargs)
    group_results = great_group(**kwargs)
    if group_results is None:
        return
    decisions.append(CompleteWork())


# @xray_recorder.capture('command_fungi_build_chain')
def _build_chain(names, **kwargs):
    get_local_ids = Signature.for_activity(names['local'], 'get_local_ids', **kwargs)
    get_remote_ids = Signature.for_activity(names['remote'], 'get_remote_ids', **kwargs)
    put_new_ids = Signature.for_activity(names['put'], 'put_new_ids', **kwargs)
    link_new_ids = Signature.for_activity(names['link'], 'link_new_ids', **kwargs)
    unlink_old_ids = Signature.for_activity(names['unlink'], 'unlink_old_ids', **kwargs)
    generate_mapping = Signature.for_activity(names['map'], 'build_mapping', **kwargs)
    get_ids_group = group(get_local_ids, get_remote_ids)
    link_group = group(link_new_ids, unlink_old_ids)
    get_change_type = Signature.for_activity(names['change_types'], 'pull_change_types', **kwargs)
    great_chain = chain(get_ids_group, put_new_ids, link_group, get_change_type, generate_mapping)
    return great_chain


# @xray_recorder.capture('command_fungi_build_group')
def _build_group(names, task_args, **kwargs):
    subtask_name = 'work_remote_id'
    work_history = kwargs['work_history']
    execution_id = kwargs['execution_id']
    remote_id_values = work_history.get_result(names['remote'])
    work_remote_ids_signatures = []
    for remote_id_value in remote_id_values['remote_id_values']:
        subtask_identifier = f'work_id-{remote_id_value}-{execution_id}'
        new_task_args = task_args.replace_argument_value(subtask_name, {'id_value': remote_id_value}, remote_id_value)
        work_remote_id_signature = Signature.for_subtask(subtask_identifier, subtask_name, new_task_args, **kwargs)
        work_remote_ids_signatures.append(work_remote_id_signature)
    tuple_signatures = tuple(work_remote_ids_signatures)
    return group(*tuple_signatures)
