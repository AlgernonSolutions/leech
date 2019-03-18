import logging

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('rough_housing')
@task('rough_housing')
def rough_housing(**kwargs):
    from toll_booth.alg_obj.aws.overseer.overseer import OverseerRecorder

    logging.info(f'received a call to fire a ruffianing task: {kwargs}')
    flow_id = kwargs['flow_id']
    overseer = OverseerRecorder(**kwargs)
    current_ruffians = overseer.get_running_ruffians(flow_id)
    execution_arns = _manage_ruffians(current_ruffians, **kwargs)
    return execution_arns


def _manage_ruffians(current_ruffians, **kwargs):
    ruffian_action = kwargs['ruffian_action']
    called_ruffians = kwargs['ruffians']
    called_ruffian_ids = set([x['ruffian_id'] for x in called_ruffians])
    if ruffian_action == 'start':
        current_ruffian_ids = set([x['ruffian_id'] for x in current_ruffians])
        pending_ruffian_ids = called_ruffian_ids - current_ruffian_ids
        pending_ruffians = [x for x in called_ruffians if x['ruffian_id'] in pending_ruffian_ids]
        return _rouse_ruffians(pending_ruffians, **kwargs)
    if ruffian_action == 'stop':
        return
    raise NotImplementedError(f'can not perform ruffian action: {ruffian_action}')


def _rouse_ruffians(pending_ruffians, **kwargs):
    from toll_booth.alg_obj.aws.ruffians.ruffian import RuffianRoost

    execution_arns = []
    for pending_ruffian in pending_ruffians:
        execution_arn = RuffianRoost.conscript_ruffian(**pending_ruffian)
        execution_arns.append(execution_arn)
    return execution_arns


def _disband_ruffians(execution_arn):
    from toll_booth.alg_obj.aws.ruffians.ruffian import RuffianRoost

    RuffianRoost.disband_ruffians(execution_arn)
