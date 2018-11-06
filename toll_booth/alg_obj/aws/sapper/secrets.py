import os

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph import InternalId


class SecretWhisperer:
    _sensitive_table_name = os.getenv('SENSITIVE_TABLE', 'Sensitives')

    @classmethod
    def get_secret(cls, internal_id):
        resource = boto3.resource('dynamodb')
        table = resource.Table(cls._sensitive_table_name)
        results = table.query(
            IndexName='string',
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=Key('insensitive').eq(internal_id)
        )
        return results['Item']['sensitive']

    @classmethod
    def put_secret(cls, sensitive_property, source_internal_id, data_type):
        resource = boto3.resource('dynamodb')
        table = resource.Table(cls._sensitive_table_name)
        id_string = ''.join([source_internal_id, data_type])
        internal_id = InternalId(id_string).id_value
        secret = {
            'insensitive': internal_id,
            'sensitive': sensitive_property
        }
        try:
            table.put_item(
                Item=secret,
                ConditionExpression=Attr('insensitive').ne(internal_id)
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
        return internal_id