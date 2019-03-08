from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('run_batch_credible_commands')
@task('run_batch_credible_commands')
def run_batch_credible_commands(**kwargs):
    pass
