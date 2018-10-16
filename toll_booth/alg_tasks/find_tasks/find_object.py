from toll_booth.alg_obj.graph.ogm.ogm import OgmReader
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def find_object(**kwargs):
    internal_id = kwargs.get('internal_id', None)
    object_type = kwargs.get('object_type', None)
    ogm = OgmReader()
    if internal_id and object_type:
        return ogm.find_object(internal_id, object_type == 'Edge')
