import logging

from toll_booth.alg_obj.forge.dentist import Dentist
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def extract(*args, **kwargs):
    logging.info('starting an extraction task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    dentist = Dentist(**task_args)
    return dentist.extract()
