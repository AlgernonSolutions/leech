from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import workflow


@xray_recorder.capture('ruffianing')
@workflow('ruffianing')
def ruffianing(**kwargs):
    pass
