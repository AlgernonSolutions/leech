from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('send_email')
@task('send_email')
def send_email(**kwargs):
    raise NotImplementedError('you need to do this')
