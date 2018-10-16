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
        if internal_id and object_type:
            if object_type in ['in_edges', 'out_edges']:
                return ogm.get_edges(internal_id, object_type == 'out_edges')
