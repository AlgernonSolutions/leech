from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import chain, LambdaSignature, group
from toll_booth.alg_tasks.rivers.rocks import workflow


@workflow('fungal_leech')
def fungal_leech(**kwargs):
    decisions = kwargs['decisions']
    execution_id = kwargs['execution_id']
    names = {
        'transform': f'transform-{execution_id}',
        'assimilate': f'assimilate-{execution_id}',
        'index': f'index-{execution_id}',
        'graph': f'graph-{execution_id}',
    }
    kwargs['names'] = names
    transform_signature = _build_transform_signature(**kwargs)
    assimilate_signature = _build_assimilate_signature(**kwargs)
    index_signature = _build_index_signature(**kwargs)
    graph_signature = _build_index_signature(**kwargs)
    load_group = group(index_signature, graph_signature)
    leech_chain = chain(transform_signature, assimilate_signature, load_group)
    leech_results = leech_chain(**kwargs)
    if leech_results is None:
        return
    decisions.append(CompleteWork())


@xray_recorder.capture('fungal_leech_build_transform_signature')
def _build_transform_signature(**kwargs):
    names = kwargs['names']
    task_identifier = names['transform']
    return LambdaSignature(task_identifier, 'transform', **kwargs)


@xray_recorder.capture('fungal_leech_build_assimilate_signature')
def _build_assimilate_signature(**kwargs):
    names = kwargs['names']
    task_identifier = names['assimilate']
    return LambdaSignature(task_identifier, 'assimilate', **kwargs)


@xray_recorder.capture('fungal_leech_build_index_signature')
def _build_index_signature(**kwargs):
    names = kwargs['names']
    task_identifier = names['index']
    return LambdaSignature(task_identifier, 'index', **kwargs)


@xray_recorder.capture('fungal_leech_build_graph_signature')
def _build_graph_signature(**kwargs):
    names = kwargs['names']
    task_identifier = names['graph']
    return LambdaSignature(task_identifier, 'graph', **kwargs)
