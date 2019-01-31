from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.rafts import Signature, LambdaSignature
from toll_booth.alg_tasks.rivers.rocks import workflow


@xray_recorder.capture('send_routine_reports')
@workflow('send_routine_reports')
def send_routine_reports(**kwargs):
    execution_id = kwargs['execution_id']
    kwargs['names'] = {
        'report_args': f'report_args-{execution_id}'
    }
    report_arg_signature = _build_report_args_signature(**kwargs)
    report_args = report_arg_signature(**kwargs)
    if report_args is None:
        return

    raise NotImplementedError('need to build the daily reporting feature')


def _build_report_args_signature(**kwargs):
    names = kwargs['names']
    return LambdaSignature(names['report_args'], 'get_report_args')


def _build_query_data_group(task_args, **kwargs):
    exceution_id = kwargs['execution_id']
    report_args = task_args.get_argument_value('report_args')
    report_name = task_args.get_argument_value('report_name')
    report_tags = task_args.get_argument_value('report_tags')
    report_arg = report_args[report_name]

