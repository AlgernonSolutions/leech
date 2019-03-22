import logging

import boto3

from toll_booth.alg_tasks.lambda_logging import lambda_logged


@lambda_logged
def cleaner(event, context):
    logging.info(f'started cleaner task: {event}')
    client = boto3.client('swf')
    if 'overseer_token' in event:
        if 'error' in event:
            error = event['error']
            _mark_activity_failed(client, event, error)
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
