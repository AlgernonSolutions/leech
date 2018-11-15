import logging
from collections import OrderedDict

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
from toll_booth.alg_obj.forge.lizards import MonitorLizard
from toll_booth.alg_obj.forge.mouse import MonitorMouse
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
@xray_recorder.capture('monitor_object')
def bulk_monitor(*args, **kwargs):
    logging.info('starting a monitor object task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    lizard = MonitorLizard(**task_args)
    results = lizard.bulk_monitor()
    logging.info('finished a monitor object task with results: %s' % results)
    return results
