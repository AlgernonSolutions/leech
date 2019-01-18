from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.rafts import ActivitySignature
from toll_booth.alg_tasks.rivers.rocks import workflow


@workflow
def fungal_leech(**kwargs):
    decisions = kwargs['decisions']
    execution_id = kwargs['execution_id']
    names = {
        'transform': f'transform-{execution_id}',
        'assimilate': f'assimilate-{execution_id}',
        'load': f'load-{execution_id}'
    }
    kwargs['names'] = names


@xray_recorder.capture('fungal_leech_build_transform_signature')
def _build_transform_signature(**kwargs):
    names = kwargs['names']
    task_identifier = names['transform']
    return ActivitySignature(task_identifier, 'transform', **kwargs)


@xray_recorder.capture('fungal_leech_build_assimilate_signature')
def _build_assimilate_signature(**kwargs):
    names = kwargs['names']
    task_identifier = names['assimilate']
    return ActivitySignature(task_identifier, 'assimilate', **kwargs)

