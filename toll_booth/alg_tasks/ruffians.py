import json
import logging

from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder
from toll_booth.alg_tasks.lambda_logging import lambda_logged
from toll_booth.alg_tasks.rivers.tasks.automation import automation_tasks


def _set_run_id_logging(flow_id, run_id, task_id, context):
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging.basicConfig(format='[%(levelname)s] || ' +
                               f'function_name:{context.function_name}|function_arn:{context.invoked_function_arn}'
                               f'|request_id:{context.aws_request_id}' +
                               f'|flow_id:{flow_id}|run_id:{run_id}|task_id:{task_id}'
                               f'|| %(asctime)s %(message)s', level=logging.INFO)


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
        flow_id, run_id, task_id = event['flow_id'], event['run_id'], event['task_id']
        _set_run_id_logging(flow_id, run_id, task_id, context)
        logging.info(f'raw task_args for {task_name} are {event["task_args"]}')
        task_args = json.loads(json.dumps(event['task_args']), cls=AlgDecoder)
        register_results = event.get('register_results', False)
        if register_results is True:
            try:
                results = production_fn(task_name, task_args)
                results = json.dumps(results, cls=AlgEncoder)
                return results
            except Exception as e:
                import traceback

                logging.error(f'failure running lambda task named {task_name}, task_args {task_args}, cause: {str(e.args)}')
                trace = traceback.format_exc()
                return json.dumps({'fail': True, 'reason': str(e.args), 'details': trace})
        results = production_fn(task_name, task_args)
        return json.dumps(results, cls=AlgEncoder)
    return wrapper


@lambda_logged
@rough_work
def decide(event, context):
    from toll_booth.alg_obj.aws.ruffians.ruffian import Ruffian

    logging.info(f'received a call to start a deciding ruffian: {event}')
    if 'warn_seconds' not in event:
        event['warn_seconds'] = 60
    ruffian = Ruffian.build(context, **event)
    ruffian.supervise()


@lambda_logged
@rough_work
def labor(event, context):
    from toll_booth.alg_obj.aws.ruffians.ruffian import Ruffian

    logging.info(f'received a call to start a working ruffian: {event}')
    ruffian = Ruffian.build(context, **event)
    ruffian.labor()


@lambda_logged
@lambda_work
def lambda_labor(task_name, task_args):
    from toll_booth.alg_tasks.rivers.tasks.fungi import fungi_tasks
    from toll_booth.alg_tasks.rivers.tasks.leech import leech_tasks
    from toll_booth.alg_tasks.rivers.tasks.posts import email_tasks
    from toll_booth.alg_tasks.rivers.tasks.posts import credible_fe_tasks

    logging.info(f'received a call to run a lambda task named {task_name}, the task_args are {task_args}')
    task_modules = [fungi_tasks, leech_tasks, email_tasks, credible_fe_tasks, automation_tasks]
    for task_module in task_modules:
        task_fn = getattr(task_module, task_name, None)
        if task_fn:
            results = task_fn(**task_args.for_task)
            logging.info(f'completed a lambda task named {task_name}, task_args {task_args}, results: {results}')
            return results
    raise NotImplementedError('could not find a registered task for type: %s' % task_name)
