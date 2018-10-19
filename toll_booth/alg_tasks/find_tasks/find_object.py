import logging

from toll_booth.alg_obj.graph.ogm.ogm import OgmReader
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def find_object(*args, **kwargs):
    logging.info(f'started a find object call with args {args} and kwargs {kwargs}')
    task_args = kwargs['task_args']
    function_args = task_args['context']
    source = task_args['source']
    if source is None:
        source = {}
    if function_args is None:
        function_args = {}
    ogm = OgmReader()
    resolver = _derive_resolver(source, function_args, ogm)
    return resolver(source, function_args)


def _derive_resolver(source, function_args, ogm):
    object_type_key = 'object_type'
    object_type = source.get(object_type_key, function_args.get(object_type_key, None))
    if not object_type:
        raise RuntimeError(f'no object type specified for this query, source: {source}, function_args: {function_args}')
    if object_type == 'Vertex':
        return ogm.get_vertex
    if object_type == 'VertexProperties':
        return ogm.get_vertex_properties
    if object_type == 'ConnectedEdges':
        return ogm.get_edge_connection
