from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('setup_for_credible_workflow')
@task('setup_for_credible_workflow')
def setup_for_credible_workflow(**kwargs):
    from toll_booth.alg_obj.aws.continuum.q import Q
    Q.create_task_queue('credible')
