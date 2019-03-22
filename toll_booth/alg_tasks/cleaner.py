import logging

import boto3
from botocore.exceptions import ClientError

from toll_booth.alg_tasks.lambda_logging import lambda_logged


@lambda_logged
def cleaner(event, context):
    logging.info(f'started cleaner task: {event}')
    client = boto3.client('swf')
    overseer_token = event.get('overseer_token')
    if overseer_token:
        if 'error' in event:
            error = event['error']
            try:
                _mark_activity_failed(client, event, error)
            except ClientError as e:
                if e.response['Error']['Code'] != 'UnknownResourceFault':
                    raise e
            return
        client.respond_activity_task_completed(
            taskToken=event['overseer_token']
        )


def _mark_activity_failed(client, event, error):
    client.respond_activity_task_failed(
        taskToken=event['overseer_token'],
        reason=error.get('Error', 'reason not specified'),
        details=error.get('Cause', 'no details specified')
    )
