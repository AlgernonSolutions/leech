from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('send_email')
@task('send_email')
def send_email(**kwargs):
    raise NotImplementedError('you need to do this')


@xray_recorder.capture('build_reports')
@task('build_reports')
def build_reports(**kwargs):
    raise NotImplementedError('you need to do this')


@xray_recorder.capture('query_data')
@task('query_data')
def query_data(**kwargs):
    raise NotImplementedError('you need to do this')
