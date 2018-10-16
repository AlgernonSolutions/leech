import logging

from toll_booth.alg_obj.graph.ogm.ogm import OgmReader
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def find_object(*args, **kwargs):
    logging.info(f'started a find object call with args {args} and kwargs {kwargs}')
    task_args = kwargs['task_args']
    internal_id = task_args.get('internal_id', None)
    object_type = task_args.get('object_type', None)
    ogm = OgmReader()
    if internal_id and object_type:
        return ogm.find_object(internal_id, object_type == 'Edge')
