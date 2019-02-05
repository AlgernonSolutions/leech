from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import LambdaSignature, ActivitySignature, group, chain
from toll_booth.alg_tasks.rivers.rocks import workflow


# @xray_recorder.capture('send_routine_reports')
@workflow('send_routine_reports')
def send_routine_reports(**kwargs):
    execution_id = kwargs['execution_id']
    decisions = kwargs['decisions']
    kwargs['names'] = {
        'report_args': f'report_args-{execution_id}'
    }
    report_arg_signature = _build_report_args_signature(**kwargs)
    report_args = report_arg_signature(**kwargs)
    if report_args is None:
        return
    query_group = _build_query_data_group(**kwargs)
    if query_group is None:
        decisions.append(CompleteWork())
    query_results = query_group(**kwargs)
    if query_results is None:
        return
    build_send_chain = _create_build_and_send_chain(**kwargs)
    final_results = build_send_chain(**kwargs)
    if final_results is None:
        return
    decisions.append(CompleteWork(final_results))


def _build_report_args_signature(task_args, **kwargs):
    names = kwargs['names']
    return LambdaSignature(names['report_args'], 'get_report_args', **kwargs)


def _build_query_data_group(task_args, **kwargs):
    execution_id = kwargs['execution_id']
    task_name = 'query_credible_data'
    report_args = task_args.get_argument_value('report_args')
    signatures = []
    for query_name, query_args in report_args['queries'].items():
        activity_id = f'{query_name}-{execution_id}'
        new_task_args = task_args.replace_argument_value(task_name, query_args, activity_id)
        signature = ActivitySignature(activity_id, task_name, task_args=new_task_args, **kwargs)
        signatures.append(signature)
    if not signatures:
        return None
    return group(*tuple(signatures))


def _create_build_and_send_chain(task_args, **kwargs):
    execution_id = kwargs['execution_id']
    build_task_name = 'build_reports'
    send_task_name = 'send_reports'
    build_id = f'{build_task_name}-{execution_id}'
    send_id = f'{send_task_name}-{execution_id}'
    build_signature = ActivitySignature(build_id, build_task_name, **kwargs)
    send_signature = ActivitySignature(send_id, send_task_name, **kwargs)
    return chain(build_signature, send_signature)
