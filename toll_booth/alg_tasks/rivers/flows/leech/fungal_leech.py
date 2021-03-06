from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import chain, LambdaSignature, group, ActivitySignature
from toll_booth.alg_tasks.rivers.rocks import workflow


@xray_recorder.capture('fungal_leech')
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
    graph_signature = _build_graph_signature(**kwargs)
    load_group = group(index_signature, graph_signature)
    load_results = load_group(**kwargs)
    if load_results is None:
        return
    decisions.append(CompleteWork())


@xray_recorder.capture('fungal_leech_build_transform_signature')
def _build_transform_signature(**kwargs):
    names = kwargs['names']
    task_identifier = names['transform']
    return ActivitySignature(task_identifier, 'transform', **kwargs)


@xray_recorder.capture('fungal_leech_build_assimilate_signature')
def _build_assimilate_group(task_args, **kwargs):
    batch_size = 100
    signatures = []
    execution_id = kwargs['execution_id']
    task_name = 'assimilate'
    transform_results = task_args.get_argument_value('transform')
    batches = []
    batch = []
    for transform_result in transform_results:
        potentials = transform_result['potentials']
        source_vertex = transform_result['source_vertex']
        extracted_data = transform_result['extracted_data']
        for potential in potentials:
            if len(batch) > batch_size:
                batches.append(batch)
                batch = []
            new_arg = {
                'rule_entry': potential[1],
                'potential_vertex': potential[0],
                'source_vertex': source_vertex,
                'extracted_data': extracted_data
            }
            batch.append(new_arg)
    if batch:
        batches.append(batch)
    for batch in batches:
        task_identifier = f'assimilate-{batches.index(batch)}-{execution_id}'
        new_task_args = task_args.replace_argument_value(task_name, {'assimilation_batch': batch}, task_identifier)
        signature = ActivitySignature(task_identifier, task_name, task_args=new_task_args, **kwargs)
        signatures.append(signature)
    tuple_signatures = tuple(signatures)
    return group(*tuple_signatures)


@xray_recorder.capture('fungal_leech_build_index_group')
def _build_index_signature(**kwargs):
    names = kwargs['names']
    index_task_identifier = names['index']
    index_signature = ActivitySignature(index_task_identifier, 'index', **kwargs)
    return index_signature


@xray_recorder.capture('fungal_leech_build_graph_chain')
def _build_graph_signature(**kwargs):
    names = kwargs['names']
    graph_task_identifier = names['graph']
    graph_signature = ActivitySignature(graph_task_identifier, 'graph', **kwargs)
    return graph_signature
