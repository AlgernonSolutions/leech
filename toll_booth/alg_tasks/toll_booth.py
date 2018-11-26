import json
import logging

from toll_booth.alg_obj.serializers import GqlDecoder
from toll_booth.alg_tasks.lambda_logging import lambda_logged
from toll_booth.alg_tasks.worker import Worker


@lambda_logged
def work(event, context):
    """
    the entry point for lambda functions that perform data modification or are compute intensive
    :param event:
        the standard input for an AWS lambda function, will contain all the
        information needed to execute the function
    :param context:
        the standard meta-data input for an AWS lambda function,
        used in this function to ascertain the amount of time remaining before timeout
    :return: obj
        on a successful operation, may or may not return the results of the operation
        if no results are returned, returns True
        if an exception occurs, returns the exception as a string
        if no results are returned, and no exception is raised, the function may return an exit code or False
    """
    logging.info('event is: %s' % event)
    results = Worker.work(event, context)
    logging.info('completed the work with results: %s' % results)
    return results


@lambda_logged
def queued(event, context):
    import json
    from toll_booth.alg_obj.serializers import AlgDecoder

    records = event['Records']
    results = []
    for record in records:
        body = json.loads(record['body'], cls=AlgDecoder)
        results.append(work(body, context))
    return results


@lambda_logged
def aphid(event, context):
    logging.info(f'starting an aphid call with event: {event}')
    event = {
        'task_name': 'find_object',
        'task_args': event
    }
    work_results = work(event, context)
    results = json.loads(work_results, cls=GqlDecoder)
    logging.info(f'aphid call completed, results: {results}')
    return results


@lambda_logged
def exploded(event, context):
    logging.info('received a call to process a dynamo entry, event: %s' % event)

    records = event['Records']
    new_event = {
        'task_name': 'explode',
        'task_args': {
            'records': records
        }
    }
    work_results = work(new_event, context)
    logging.info('completed a call to process a dynamo entry, results: %s' % work_results)
    return work_results


@lambda_logged
def propagate(event, context):
    logging.info(f'starting a propagate call with event: {event}')
    event = event.update({'context': context})
    event = {
        'task_name': 'propagate',
        'task_args': event
    }
    work_results = work(event, context)
    logging.info('completed a propagate call, results: %s' % work_results)
    return work_results


@lambda_logged
def fruit(event, context):
    logging.info(f'starting a fruit call with event: {event}')
    task_args = event['task_args']
    propagation_id = event['propagation_id']
    task_args['context'] = context
    task_args['propagation_id'] = propagation_id
    event = {
        'task_name': 'fruit',
        'task_args': task_args
    }
    work_results = work(event, context)
    logging.info('completed a fruit call, results: %s' % work_results)
    return work_results
