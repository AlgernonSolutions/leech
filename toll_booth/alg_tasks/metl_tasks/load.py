import logging

from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.sapper.dynamo_driver import LeechDriver
from toll_booth.alg_obj.graph.ogm.ogm import Ogm
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def load(*args, **kwargs):
    logging.info('starting a load task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    dynamo_driver = LeechDriver(**task_args)
    key_fields = task_args['keys']
    keys = {
        'identifier_stem': key_fields['identifier_stem'],
        'id_value': key_fields['sid_value']
    }
    potential_object = dynamo_driver.get_object(**keys)
    ogm = Ogm(**task_args)
    graph_results = ogm.graph_object(potential_object)
    try:
        dynamo_driver.mark_object_as_graphed(
            identifier_stem=potential_object['source'].identifier_stem,
            id_value=potential_object['source'].id_value)
    except ClientError as e:
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise e
        logging.warning(
            'attempted to mark a vertex as graphing, '
            'but it appears this step has already happened, no changes to be made' % potential_object)
    return graph_results
