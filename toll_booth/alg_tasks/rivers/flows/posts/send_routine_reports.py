from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import workflow


@xray_recorder.capture('send_routine_reports')
@workflow('send_routine_reports')
def send_routine_reports(**kwargs):
    raise NotImplementedError('need to build the daily reporting feature')
