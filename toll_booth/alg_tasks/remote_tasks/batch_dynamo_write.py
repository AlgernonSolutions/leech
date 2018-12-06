import logging

from botocore.exceptions import ClientError


def batch_dynamo_write(*args, **kwargs):
    import boto3

    logging.info('firing the batch_dynamo_write command, with args/kwargs: %s/%s' % (args, kwargs))
    task_args = args[0]
    table_name = task_args['table_name']
    entries = task_args['entries']
    table = boto3.resource('dynamodb').Table(table_name)
    results = {'success': [], 'failed': []}
    try:
        with table.batch_writer(overwrite_by_pkeys=['sid_value', 'identifier_stem']) as writer:
            for entry in entries:
                writer.put_item(**entry['item'])
                results['success'].append(entry['id'])
    except ClientError as e:
        for entry in entries:
            results['failed'].append({'exception': e.response, 'id': entry['id']})
    return results
