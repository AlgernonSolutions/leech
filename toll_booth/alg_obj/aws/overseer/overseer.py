import os

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.ruffians.ruffian import RuffianId
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


class Overseer:
    def __init__(self, **kwargs):
        table_name = kwargs.get('table_name')
        if not table_name:
            table_name = os.getenv('OVERSEER_TABLE', 'overseer')
        self._table_name = table_name
        self._table = boto3.resource('dyanmodb').Table(table_name)
        client = boto3.client('dynamodb')
        self._query_paginator = client.get_paginator('query')

    def record_ruffian_start(self, ruffian_id: RuffianId, execution_arn, start_time):
        item = ruffian_id.as_overseer_item(execution_arn, start_time)
        try:
            response = self._table.put_item(
                Item=item,
                ConditionExpression=Attr('execution_arn').not_exists() & Attr('run_identifier_stem').not_exists()
            )
            return response
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
            raise RuntimeError(f'attempted to record the start of a ruffian: {ruffian_id.as_overseer_key(execution_arn)}, '
                               f'but an entry for that ruffian already exists')

    def record_ruffian_end(self, ruffian_id: RuffianId, execution_arn, end_time):
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
            KeyConditionExpression=Key('flow_id').eq(flow_id),
            FilterExpression=Attr('running').eq(True)
        )
        for page in response:
            ruffians.extend([{'execution_arn': x['execution_arn'], 'ruffian_id': RuffianId(**x)}] for x in page['Items'])
        return ruffians
