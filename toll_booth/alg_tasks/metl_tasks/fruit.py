import logging

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.forge.fungi import Mycelium
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
@xray_recorder.capture('monitor_object')
def fruit(*args, **kwargs):
    logging.info('starting a fruit task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    shroom = Mycelium(**task_args)
    results = shroom.creep()
    logging.info('finished a fruit object task with results: %s' % results)
    return results
