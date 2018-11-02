import json
import logging

from toll_booth.alg_obj.serializers import GqlDecoder
from toll_booth.alg_tasks.worker import Worker


def lambda_logged(lambda_function):
    def wrapper(*args):
        event = args[0]
        context = args[1]
        root = logging.getLogger()
        if root.handlers:
            for handler in root.handlers:
                root.removeHandler(handler)
        logging.basicConfig(format='[%(levelname)s] ||' +
                                   f'function_name:{context.function_name}|function_arn{context.invoked_function_arn}|request_id:{context.aws_request_id}' +
                                   '|| %(asctime)s %(message)s', level=logging.INFO)
        logging.getLogger('aws_xray_sdk').setLevel(logging.WARNING)
        logging.getLogger('botocore').setLevel(logging.WARNING)

        return lambda_function(event, context)

    return wrapper


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
