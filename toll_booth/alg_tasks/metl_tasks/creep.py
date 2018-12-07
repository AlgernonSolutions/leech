import logging

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.forge.fungi import Mycelium
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
@xray_recorder.capture('mycelium_creep')
def creep(*args, **kwargs):
    logging.info('starting a creep task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    mycelium = Mycelium(**task_args)
    results = mycelium.creep()
    logging.info('finished a creep object task with results: %s' % results)
    return results
