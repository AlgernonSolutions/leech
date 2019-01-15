import json
import logging

from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder
from toll_booth.alg_tasks.lambda_logging import lambda_logged


def rough_work(production_fn):
    def wrapper(event, context):
        event = json.loads(json.dumps(event, cls=AlgEncoder), cls=AlgDecoder)
        return production_fn(event, context)
    return wrapper


@lambda_logged
@rough_work
def decide(event, context):
    from toll_booth.alg_obj.aws.ruffians.ruffian import Ruffian

    if 'warn_seconds' not in event:
        event['warn_seconds'] = 60
    event['work_lists'] = event['decider_list']
    ruffian = Ruffian.build(context, **event)
    ruffian.supervise()


@lambda_logged
@rough_work
def labor(event, context):
    from toll_booth.alg_obj.aws.ruffians.ruffian import Ruffian

    ruffian = Ruffian.build(context, **event)
    ruffian.labor()


@lambda_logged
def lambda_labor(event, context):
    from toll_booth.alg_tasks.rivers.tasks.fungi import fungi_tasks

    task_name = event['task_name']
    task_args = json.loads(json.dumps(event['task_args'], cls=AlgEncoder), cls=AlgDecoder)
    logging.info(f'received a call to run a lambda task named {task_name}, the task_args are {task_args}')
    task_modules = [fungi_tasks]
    for task_module in task_modules:
        task_fn = getattr(task_module, task_name, None)
        if task_fn:
            results = task_fn(**task_args)
            return results
    raise NotImplementedError('could not find a registered task for type: %s' % task_name)
