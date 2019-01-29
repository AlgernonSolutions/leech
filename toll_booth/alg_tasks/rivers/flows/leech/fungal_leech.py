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
        'index': f'index-{execution_id}',
        'graph': f'graph-{execution_id}',
    }
    kwargs['names'] = names
    transform_signature = _build_transform_signature(**kwargs)
    transform_results = transform_signature(**kwargs)
    if transform_results is None:
        return
    assimilate_group = _build_assimilate_group(**kwargs)
    assimilate_results = assimilate_group(**kwargs)
    if assimilate_results is None:
        return
    index_signature = _build_index_signature(**kwargs)
    graph_signature = _build_index_signature(**kwargs)
    load_group = group(index_signature, graph_signature)
    load_results = load_group(**kwargs)
    if load_results is None:
        return
    decisions.append(CompleteWork())


@xray_recorder.capture('fungal_leech_build_transform_signature')
def _build_transform_signature(**kwargs):
    names = kwargs['names']
    task_identifier = names['transform']
    return LambdaSignature(task_identifier, 'transform', **kwargs)


@xray_recorder.capture('fungal_leech_build_assimilate_signature')
def _build_assimilate_group(task_args, **kwargs):
    signatures = []
    execution_id = kwargs['execution_id']
    task_name = 'assimilate'
    potentials = task_args.get_argument_value('potentials')
    for potential in potentials:
        new_arg = {'rule_entry': potential[1], 'source_vertex': potential[0]}
        new_task_args = task_args.replace_argument_value(task_name, new_arg, str(new_arg))
        task_identifier = f'assimilate-{str(new_arg)}-{execution_id}'
        signature = LambdaSignature(task_identifier, task_name, task_args=new_task_args, **kwargs)
        signatures.append(signature)
    tuple_signatures = tuple(signatures)
    return group(*tuple_signatures)


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
