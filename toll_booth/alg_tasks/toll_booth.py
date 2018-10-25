import json
import logging

from toll_booth.alg_obj.serializers import GqlDecoder
from toll_booth.alg_tasks.worker import Worker

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)
logging.basicConfig(format='[%(levelname)s] %(asctime)s %(message)s', level=logging.INFO)
logging.getLogger('aws_xray_sdk').setLevel(logging.INFO)


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


def queued(event, context):
    import json
    from toll_booth.alg_obj.serializers import AlgDecoder

    records = event['Records']
    results = []
    for record in records:
        body = json.loads(record['body'], cls=AlgDecoder)
        results.append(work(body, context))
    return results


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


def exploded(event, context):
    demo = {
        'Records': [
            {
                'eventID': '616bbf393f3512ff1ee8cb39446815ba',
                'eventName': 'MODIFY',
                'eventVersion': '1.1',
                'eventSource': 'aws:dynamodb',
                'awsRegion': 'us-east-1',
                'dynamodb': {
                    'ApproximateCreationDateTime': 1540406760.0,
                    'Keys': {
                        'identifier_stem': {
                            'S': 'algernon.Email'
                        },
                        'id_value': {
                            'N': '1001'
                        }
                    },
                    'NewImage': {
                        'internal_id': {
                            'S': '12345'
                        },
                        'object_type': {
                            'S': 'ExternalId'
                        },
                        'identifier_stem': {
                            'S': 'algernon.Email'
                        },
                        'id_value': {
                            'N': '1001'
                        }
                    },
                    'OldImage': {
                        'internal_id': {
                            'S': '1234'
                        },
                        'object_type': {
                            'S': 'ExternalId'
                        },
                        'identifier_stem': {
                            'S': 'algernon.Email'
                        },
                        'id_value': {
                            'N': '1001'
                        }
                    },
                    'SequenceNumber': '4432700000000062492661552',
                    'SizeBytes': 193,
                    'StreamViewType': 'NEW_AND_OLD_IMAGES'
                },
                'eventSourceARN': 'arn:aws:dynamodb:us-east-1:803040539655:table/Seeds/stream/2018-10-24T18:41:45.633'
            }
        ]
    }
    logging.info('received a call to process a dynamo entry, event: %s' % event)


def _map_aphid(event):
    resource = event['resource']
    method = event['httpMethod']
    if method == 'POST':
        if resource == '/schemata/schema':
            return 'update_schema'
        if resource == '/schemata/pattern':
            return 'update_pattern'
    if method == 'GET':
        if resource == '/schemata/schema':
            return 'get_schema'
