import logging

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.forge.fungi import Spore
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
@xray_recorder.capture('monitor_object')
def propagate(*args, **kwargs):
    logging.info('starting a propagate object task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    shroom = Spore(**task_args)
    results = shroom.propagate()
    logging.info('finished a propagate object task with results: %s' % results)
    return results
