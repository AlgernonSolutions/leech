import json
import logging

from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder
from toll_booth.alg_tasks.lambda_logging import lambda_logged


def rough_work(production_fn):
    def wrapper(event, context):
        logging.info(f'started a rough_work decorated function: {production_fn}, event: {event}')

        event = json.loads(json.dumps(event, cls=AlgEncoder), cls=AlgDecoder)
        return production_fn(event, context)
    return wrapper


def lambda_work(production_fn):
    def wrapper(event, context):
        logging.info(f'started a lambda_work decorated function: {production_fn}, event: {event}')

        task_name = event['task_name']
        logging.info(f'raw task_args for {task_name} are {event["task_args"]}')
        task_args = json.loads(json.dumps(event['task_args']), cls=AlgDecoder)
        register_results = event.get('register_results', False)
        if register_results is True:
            try:
                task_args = _manually_rehydrate_task_args(task_args)
                results = production_fn(task_name, task_args)
                results = json.dumps(results, cls=AlgEncoder)
                return results
            except Exception as e:
                import traceback

                logging.info(f'failure running lambda task named {task_name}, task_args {task_args}, cause: {str(e.args)}')
                trace = traceback.format_exc()
                return json.dumps({'fail': True, 'reason': str(e.args), 'details': trace})
        results = production_fn(task_name, task_args)
        return json.dumps(results, cls=AlgEncoder)
    return wrapper


def _manually_rehydrate_task_args(task_arg_dict):
    from toll_booth.alg_obj.aws.snakes.snakes import StoredData
    from toll_booth.alg_obj.aws.gentlemen.tasks import TaskArguments
    arguments = {}
    argument_values = task_arg_dict['value']
    for operation_name, snake_dict in argument_values['_arguments'].items():
        pointer = snake_dict['value']['pointer']
        stored_data = StoredData.retrieve(pointer)
        arguments[operation_name] = stored_data
    return TaskArguments(arguments)


@lambda_logged
@rough_work
def decide(event, context):
    from toll_booth.alg_obj.aws.ruffians.ruffian import Ruffian

    if 'warn_seconds' not in event:
        event['warn_seconds'] = 60
    ruffian = Ruffian.build(context, **event)
    ruffian.supervise()


@lambda_logged
@rough_work
def labor(event, context):
    from toll_booth.alg_obj.aws.ruffians.ruffian import Ruffian

    ruffian = Ruffian.build(context, **event)
    ruffian.labor()


@lambda_logged
@lambda_work
def lambda_labor(task_name, task_args):
    from toll_booth.alg_tasks.rivers.tasks.fungi import fungi_tasks
    logging.info(f'received a call to run a lambda task named {task_name}, the task_args are {task_args}')
    task_modules = [fungi_tasks]
    for task_module in task_modules:
        task_fn = getattr(task_module, task_name, None)
        if task_fn:
            results = task_fn(**task_args.for_task)
            logging.info(f'completed a lambda task named {task_name}, task_args {task_args}, results: {results}')
            return results
    raise NotImplementedError('could not find a registered task for type: %s' % task_name)
