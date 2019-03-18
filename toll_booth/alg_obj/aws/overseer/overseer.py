import os

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


class OverseerRecorder:
    def __init__(self, **kwargs):
        table_name = kwargs.get('table_name')
        if not table_name:
            table_name = os.getenv('OVERSEER_TABLE', 'overseer')
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(table_name)
        client = boto3.client('dynamodb')
        self._query_paginator = client.get_paginator('query')

    def record_ruffian_start(self, ruffian_id, execution_arn, start_time):
        item = ruffian_id.as_overseer_item(execution_arn, start_time)
        try:
            response = self._table.put_item(
                Item=item,
                ConditionExpression=Attr('workflow_id').not_exists() & Attr('ruffian_id').not_exists()
            )
            return response
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
            raise RuntimeError(f'attempted to record the start of a ruffian: {ruffian_id.as_overseer_key(execution_arn)}, '
                               f'but an entry for that ruffian already exists')

    def record_ruffian_end(self, ruffian_id, execution_arn, end_time):
        key = ruffian_id.as_overseer_key(execution_arn)
        response = self._table.update_item(
            Key=key,
            UpdateExpression='SET end_time=:et, running=:r',
            ExpressionAttributeValues={':et': end_time, ':r': False}
        )
        return response

    def get_running_ruffians(self, flow_id):
        ruffians = []
        response = self._query_paginator.paginate(
            TableName=self._table_name,
            KeyConditionExpression='workflow_id=:fi',
            FilterExpression='running=:f',
            ExpressionAttributeValues={':fi': {'S': flow_id}, ':f': {'BOOL': True}}
        )
        for page in response:
            ruffians.extend([{'execution_arn': x['execution_arn'], 'ruffian_id': x['ruffian_id']}] for x in page['Items'])
        return ruffians
