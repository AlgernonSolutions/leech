# TODO implement this
"""
    this is the master workflow module,
    it contains the highest level orchestration logic needed to operate the Leech
"""
from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.rafts import ActivitySignature
from toll_booth.alg_tasks.rivers.rocks import workflow


@xray_recorder.capture('command_fungi')
@workflow('leech')
def leech(**kwargs):
    """
        the main workflow for the leech,
        assumes that the objects in the remote source can be accessed directly by unique key
    :return:
    """
    pass


def _build_monitor_signature(**kwargs):
    execution_id = kwargs['execution_id']
    monitor_id = f'monitor-{execution_id}'
    extract_id = f'extract-{execution_id}'
    monitor_signal = ActivitySignature(monitor_id, 'monitor', **kwargs)
    extract_signature = ActivitySignature(extract_id, 'extract', **kwargs)
