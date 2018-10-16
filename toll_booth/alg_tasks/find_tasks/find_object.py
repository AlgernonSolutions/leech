import logging

from toll_booth.alg_obj.graph.ogm.ogm import OgmReader
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def find_object(*args, **kwargs):
    logging.info(f'started a find object call with args {args} and kwargs {kwargs}')
    task_args = kwargs['task_args']
    context = task_args['context']
    source = task_args['source']
    ogm = OgmReader()
    if context:
        internal_id = context.get('internal_id', None)
        object_type = context.get('object_type', None)
        if internal_id and object_type:
            return ogm.find_object(internal_id, object_type == 'Edge')
    if source:
        object_type = source['object_type']
        internal_id = source['internal_id']
        neighbors = source['neighbors']
        if internal_id and object_type:
            out = neighbors in ['out_edges', 'out_vertexes']
            is_edge = object_type == 'Edge'
            get_edges = neighbors in ['out_edges', 'in_edges']
            return ogm.get_neighbors(
                internal_id=internal_id, out=out, is_edge=is_edge, get_edges=get_edges
            )
