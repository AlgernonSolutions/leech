from toll_booth.alg_obj.forge.forklifts import ReachTruck
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def load(*args, **kwargs):
    print('starting a transform task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    reach_truck = ReachTruck(**task_args)
    return reach_truck.load()
