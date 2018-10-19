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
    property_type_key = 'property'
    object_type = source.get(object_type_key, function_args.get(object_type_key, None))
    object_property = source.get(property_type_key, function_args.get(property_type_key, None))
    if not object_type:
        raise RuntimeError(f'no object type specified for this query, source: {source}, function_args: {function_args}')
    if object_type == 'Vertex':
        if object_property == 'ConnectedEdges':
            return ogm.get_edge_connection
        if object_property == 'VertexProperties':
            return ogm.get_vertex_properties
        return ogm.get_vertex
