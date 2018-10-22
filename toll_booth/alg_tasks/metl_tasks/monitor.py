import logging

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.forge.lizards import MonitorLizard
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
@xray_recorder.capture('monitor')
def monitor(*args, **kwargs):
    logging.info('starting a monitoring task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    logging.info('starting to build the lizard')
    lizard = MonitorLizard(**task_args)
    logging.info('built the lizard')
    return lizard.monitor()
