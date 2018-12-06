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
        with table.batch_writer() as writer:
            for entry in entries:
                writer.put_item(**entry)
                results['success'].append(entry['Item']['sid_value'])
    except ClientError as e:
        for entry in entries:
            results['failed'].append({'exception': e.response, 'Id': entry['Item']['sid_value']})
    return results
