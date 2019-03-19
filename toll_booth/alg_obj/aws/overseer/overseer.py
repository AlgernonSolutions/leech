import json
import os
import uuid

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.gentlemen.tasks import Versions
from toll_booth.alg_obj.aws.moorland import cos
from toll_booth.alg_obj.aws.ruffians.ruffian import RuffianId, RuffianRoost
from toll_booth.alg_obj.serializers import AlgEncoder


class Overseer:
    def __init__(self, domain_name, overseer_run_id):
        self._domain_name = domain_name
        self._overseer_run_id = overseer_run_id

    @classmethod
    def start(cls, domain_name, versions=None):
        if not versions:
            versions = Versions.retrieve(domain_name)
        client = boto3.client('swf')
        flow = 'ruffianing'
        start_args = {
            'domain': domain_name,
            'workflowId': flow,
            'workflowType': {
                'name': flow,
                'version': str(versions.workflow_versions[flow])
            },
            'taskList': {'name': flow},
            'childPolicy': 'TERMINATE',
            'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
        }
        current_overseer_id = cos.getenv('global_overseer_run_id')
        try:
            overseer_ruffian_id = RuffianId.for_overseer(domain_name)
            start_results = client.start_workflow_execution(**start_args)
            run_id = start_results['runId']
            cos.environ['overseer_run_id'] = run_id
            RuffianRoost.conscript_ruffian(overseer_ruffian_id)
            current_overseer_id = run_id
        except ClientError as e:
            if e.response['Error']['Code'] != 'WorkflowExecutionAlreadyStartedFault':
                raise e
        return cls(domain_name, current_overseer_id)

    def signal(self, flow_id, leech_config, ruffians):
        client = boto3.client('swf')
        client.signal_workflow_execution(
            domain=self._domain_name,
            workflowId='ruffianing',
            runId=self._overseer_run_id,
            signalName='start_ruffian',
            input=json.dumps({
                'signal_id': uuid.uuid4().hex,
                'flow_id': flow_id,
                'ruffians': ruffians,
                'config': leech_config
            }, cls=AlgEncoder)
        )


class OverseerRecorder:
    def __init__(self, **kwargs):
        table_name = kwargs.get('table_name')
        if not table_name:
            table_name = os.getenv('OVERSEER_TABLE', 'overseer')
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(table_name)
        client = boto3.client('dynamodb')
        self._query_paginator = client.get_paginator('query')

    def record_ruffian_start(self,  ruffian_id, execution_arn, start_time):
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

    def record_ruffian_end(self, ruffian_id, end_time):
        key = ruffian_id.as_overseer_key
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
            for entry in page['Items']:
                ruffians.append({
                    'execution_arn': entry['execution_arn']['S'],
                    'ruffian_id': entry['ruffian_id']['S']
                })
        return ruffians
