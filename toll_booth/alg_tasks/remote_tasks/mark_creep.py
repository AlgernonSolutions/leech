import logging

from botocore.exceptions import ClientError


def mark_creep(*args, **kwargs):
    import boto3

    logging.info('firing the mark_creep command, with args/kwargs: %s/%s' % (args, kwargs))
    task_args = args[0]
    table_name = task_args['table_name']
    entries = task_args['entries']
    table = boto3.resource('dynamodb').Table(table_name)
    results = {'success': [], 'failed': []}
    try:
        with table.batch_writer(overwrite_by_pkeys=['sid_value', 'identifier_stem']) as writer:
            for entry in entries:
                write_item = entry['item']
                writer.put_item(**write_item)
                results['success'].append(entry['id'])
    except ClientError as e:
        for entry in entries:
            results['failed'].append({'exception': e.response, 'id': entry['id']})
    logging.info('completed the mark_creep_command')
    return results
