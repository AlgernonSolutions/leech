from toll_booth.alg_obj.forge.borgs import SevenOfNine
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def assimilate(*args, **kwargs):
    print('starting a transform task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    seven = SevenOfNine(**task_args)
    return seven.assimilate()
