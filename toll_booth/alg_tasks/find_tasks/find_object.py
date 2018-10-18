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
            return ogm.find_object(object_type == 'Edge', **context)
    if source:
        object_type = source['object_type']
        internal_id = source['internal_id']
        requested_property = source['property']
        if object_type and internal_id:
            if requested_property == 'ConnectedEdges':
                return ogm.get_edge_connection(**source)
