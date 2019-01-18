from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import ActivitySignature, chain
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
    transform_signature = _build_transform_signature(**kwargs)
    assimilate_signature = _build_assimilate_signature(**kwargs)
    leech_chain = chain(transform_signature, assimilate_signature)
    leech_results = leech_chain(**kwargs)
    if leech_results is None:
        return
    decisions.append(CompleteWork())


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


@xray_recorder.capture('fungal_leech_build_load_signature')
def _build_assimilate_signature(**kwargs):
    names = kwargs['names']
    task_identifier = names['load']
    return ActivitySignature(task_identifier, 'load', **kwargs)