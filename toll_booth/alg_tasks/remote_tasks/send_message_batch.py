import logging


def send_message_batch(*args, **kwargs):
    import boto3
    from botocore.config import Config

    logging.info('firing the send_message_batch command, with args/kwargs: %s/%s' % (args, kwargs))
    returned_results = {'success': [], 'failed': []}
    task_args = args[0]
    target_queue_url = task_args['target_queue_url']
    entries = task_args['entries']
    session = boto3.session.Session()

    client = session.client('sqs', config=Config(
        connect_timeout=315, read_timeout=315, max_pool_connections=25, retries={'max_attempts': 2}))
    results = client.send_message_batch(
        QueueUrl=target_queue_url,
        Entries=entries
    )
    try:
        for entry in results['Successful']:
            returned_results['success'].append({'message_id': entry['Id']})
    except KeyError:
        pass
    try:
        for entry in results['Failed']:
            returned_results['failed'].append(entry)
    except KeyError:
        pass
    return returned_results
