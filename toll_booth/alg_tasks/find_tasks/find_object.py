import logging

from toll_booth.alg_obj.graph.ogm.ogm import OgmReader
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def find_object(*args, **kwargs):
    logging.info(f'started a find object call with args {args} and kwargs {kwargs}')
    task_args = kwargs['task_args']
    function_args = task_args['context']
    source = task_args['source']
    username = task_args['username']
    object_type = task_args['object_type']
    object_property = task_args.get('object_property', None)
    if source is None:
        source = {}
    if function_args is None:
        function_args = {}
    ogm = OgmReader()
    resolver = _derive_resolver(source, function_args, object_type, object_property, ogm)
    return resolver(source, function_args, username=username)


def _derive_resolver(source, function_args, object_type, object_property, ogm):
    if object_type == 'Vertex':
        if object_property == 'ConnectedEdges':
            return ogm.get_edge_connection
        if object_property == 'VertexProperties':
            return ogm.get_vertex_properties
        return ogm.get_vertex
    if object_type == 'EdgeConnection':
        return ogm.get_edge_connection
    if object_type == 'Vertexes':
        return ogm.find_vertexes
    raise NotImplementedError(f'could not find any matching resolves for '
                              f'source: {source}, functions_args: {function_args}')
