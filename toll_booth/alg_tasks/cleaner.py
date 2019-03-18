import logging

import boto3

from toll_booth.alg_tasks.lambda_logging import lambda_logged


@lambda_logged
def cleaner(event, context):
    logging.info(f'started cleaner task: {event}')
    client = boto3.client('swf')
    if 'overseer_token' in event:
        client.respond_activity_task_failed(
            taskToken=event['overseer_token'],
            reason=event.get('reason'),
            details=event.get('error')
        )

