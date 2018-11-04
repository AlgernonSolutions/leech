import logging

from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.sapper.dynamo_driver import DynamoDriver


def bulk_dynamo_write(*args, **kwargs):
    logging.info('firing the bulk_dynamo_write command, with args/kwargs: %s/%s' % (args, kwargs))
    returned_results = {'working': [], 'not_working': []}
    task_args = args[0]
    id_values = task_args['id_values']
    identifier_stem = task_args['identifier_stem']
    object_type = task_args['object_type']
    stage_name = task_args.get('stage_name')
    dynamo_driver = DynamoDriver()
    for id_value in id_values:
        try:
            dynamo_driver.put_vertex_seed(identifier_stem, id_value, object_type, stage_name)
            returned_results['not_working'].append(id_value)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
            returned_results['working'].append(id_value)
    return returned_results
