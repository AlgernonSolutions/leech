import logging
from collections import OrderedDict

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
from toll_booth.alg_obj.forge.fungi import Spore
from toll_booth.alg_obj.forge.lizards import MonitorLizard
from toll_booth.alg_obj.forge.mouse import MonitorMouse
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
@xray_recorder.capture('monitor_object')
def propagate_object(*args, **kwargs):
    logging.info('starting a monitor object task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    shroom = Spore(**task_args)
    results = shroom.propagate()
    logging.info('finished a monitor object task with results: %s' % results)
    return results
