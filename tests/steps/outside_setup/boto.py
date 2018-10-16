import json
from datetime import datetime
from decimal import Decimal

from dateutil.tz import tzlocal

from tests.steps.schema_setup.schema_entry import MockVertexSchemaEntry, MockEdgeSchemaEntry


class MockBoto:
    @classmethod
    def get_for_monitor(cls, context):
        generic_schema_response = {
            'Items': [],
            'Count': 1,
            'ScannedCount': 1,
            'LastEvaluatedKey': {'entry_name': 'ExternalId', 'version': Decimal('10')},
            'ResponseMetadata': {
                'RequestId': 'SKPJI62R6UB5B2V0HKUKS0FN6JVV4KQNSO5AEMVJF66Q9ASUAAJG',
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'server': 'Server',
                    'date': 'Wed, 05 Sep 2018 14:37:35 GMT',
                    'content-type': 'application/x-amz-json-1.0',
                    'content-length': '1617',
                    'connection': 'keep-alive',
                    'x-amzn-requestid': 'SKPJI62R6UB5B2V0HKUKS0FN6JVV4KQNSO5AEMVJF66Q9ASUAAJG',
                    'x-amz-crc32': '1244676326'
                },
                'RetryAttempts': 0
            }
        }
        schema_entry = MockVertexSchemaEntry.get(context, as_dict=True)
        schema_entry['version'] = Decimal('10')
        schema_entry['entry_name'] = schema_entry['vertex_name']
        schema_response = generic_schema_response.copy()
        schema_response['Items'].append(schema_entry)
        secret_response = {
            'ARN': 'arn:aws:secretsmanager:us-east-1:803040539655:secret:mock_secret',
            'Name': 'mock_secret',
            'VersionId': '56bef95c-498d-4b3c-9eb0-84c64b8c3480',
            'SecretString': f'{json.dumps({"key_value": "some_mocked_api_key"})}',
            'VersionStages': ['AWSCURRENT'],
            'CreatedDate': datetime(2018, 6, 7, 10, 57, 14, 798000, tzinfo=tzlocal()),
            'ResponseMetadata': {
                'RequestId': '193c81c0-b117-11e8-b13b-d30703fcf690',
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'date': 'Wed, 05 Sep 2018 14:22:20 GMT',
                    'content-type': 'application/x-amz-json-1.1',
                    'content-length': '395',
                    'connection': 'keep-alive',
                    'x-amzn-requestid': '193c81c0-b117-11e8-b13b-d30703fcf690'
                },
                'RetryAttempts': 0
            }
        }
        return [schema_response, secret_response]


def generate_vertex_schema_entry(*args):
    generic_schema_response = {
        'Items': [],
        'Count': 1,
        'ScannedCount': 1,
        'LastEvaluatedKey': {'entry_name': 'ExternalId', 'version': Decimal('10')},
        'ResponseMetadata': {
            'RequestId': 'SKPJI62R6UB5B2V0HKUKS0FN6JVV4KQNSO5AEMVJF66Q9ASUAAJG',
            'HTTPStatusCode': 200,
            'HTTPHeaders': {
                'server': 'Server',
                'date': 'Wed, 05 Sep 2018 14:37:35 GMT',
                'content-type': 'application/x-amz-json-1.0',
                'content-length': '1617',
                'connection': 'keep-alive',
                'x-amzn-requestid': 'SKPJI62R6UB5B2V0HKUKS0FN6JVV4KQNSO5AEMVJF66Q9ASUAAJG',
                'x-amz-crc32': '1244676326'
            },
            'RetryAttempts': 0
        }
    }
    call_args = args[1]
    key_condition = call_args['KeyConditionExpression']
    object_type = getattr(key_condition, '_values')[1]
    schema_entry = MockVertexSchemaEntry.full_get(object_type, as_dict=True)
    schema_entry['version'] = Decimal('10')
    schema_entry['entry_name'] = schema_entry['vertex_name']
    generic_schema_response['Items'].append(schema_entry)
    return generic_schema_response


def generate_edge_schema_entry(*args):
    generic_schema_response = {
        'Items': [],
        'Count': 1,
        'ScannedCount': 1,
        'LastEvaluatedKey': {'entry_name': 'ExternalId', 'version': Decimal('10')},
        'ResponseMetadata': {
            'RequestId': 'SKPJI62R6UB5B2V0HKUKS0FN6JVV4KQNSO5AEMVJF66Q9ASUAAJG',
            'HTTPStatusCode': 200,
            'HTTPHeaders': {
                'server': 'Server',
                'date': 'Wed, 05 Sep 2018 14:37:35 GMT',
                'content-type': 'application/x-amz-json-1.0',
                'content-length': '1617',
                'connection': 'keep-alive',
                'x-amzn-requestid': 'SKPJI62R6UB5B2V0HKUKS0FN6JVV4KQNSO5AEMVJF66Q9ASUAAJG',
                'x-amz-crc32': '1244676326'
            },
            'RetryAttempts': 0
        }
    }
    call_args = args[1]
    key_condition = call_args['KeyConditionExpression']
    object_type = getattr(key_condition, '_values')[1]
    schema_entry = MockEdgeSchemaEntry.full_get(object_type, as_dict=True)
    schema_entry['version'] = Decimal('10')
    schema_entry['entry_name'] = schema_entry['edge_label']
    generic_schema_response['Items'].append(schema_entry)
    return generic_schema_response


def generate_schema_entry(*args):
    try:
        return generate_vertex_schema_entry(*args)
    except RuntimeError:
        return generate_edge_schema_entry(*args)


def generate_secret_response(*args):
    call_args = args[1]
    returned_secret = {'key_value': 'some_mocked_api_key'}
    if call_args['SecretId'] == 'Trident_User_Key':
        returned_secret = {
            'trident_user_key': 'some_secret_key_value',
            'trident_user_key_id': 'some_access_key_value'
        }
    secret_response = {
        'ARN': 'arn:aws:secretsmanager:us-east-1:803040539655:secret:mock_secret',
        'Name': 'mock_secret',
        'VersionId': '56bef95c-498d-4b3c-9eb0-84c64b8c3480',
        'SecretString': f'{json.dumps(returned_secret)}',
        'VersionStages': ['AWSCURRENT'],
        'CreatedDate': datetime(2018, 6, 7, 10, 57, 14, 798000, tzinfo=tzlocal()),
        'ResponseMetadata': {
            'RequestId': '193c81c0-b117-11e8-b13b-d30703fcf690',
            'HTTPStatusCode': 200,
            'HTTPHeaders': {
                'date': 'Wed, 05 Sep 2018 14:22:20 GMT',
                'content-type': 'application/x-amz-json-1.1',
                'content-length': '395',
                'connection': 'keep-alive',
                'x-amzn-requestid': '193c81c0-b117-11e8-b13b-d30703fcf690'
            },
            'RetryAttempts': 0
        }
    }
    return secret_response


def generate_sensitive_response(*args):
    generic_schema_response = {
        'Items': [],
        'Count': 1,
        'ScannedCount': 1,
        'LastEvaluatedKey': {'entry_name': 'ExternalId', 'version': Decimal('10')},
        'ResponseMetadata': {
            'RequestId': 'SKPJI62R6UB5B2V0HKUKS0FN6JVV4KQNSO5AEMVJF66Q9ASUAAJG',
            'HTTPStatusCode': 200,
            'HTTPHeaders': {
                'server': 'Server',
                'date': 'Wed, 05 Sep 2018 14:37:35 GMT',
                'content-type': 'application/x-amz-json-1.0',
                'content-length': '1617',
                'connection': 'keep-alive',
                'x-amzn-requestid': 'SKPJI62R6UB5B2V0HKUKS0FN6JVV4KQNSO5AEMVJF66Q9ASUAAJG',
                'x-amz-crc32': '1244676326'
            },
            'RetryAttempts': 0
        }
    }
    call_args = args[1]
    key = call_args['Key']
    print()


def intercept(*args):
    operation_name = args[0]
    if operation_name == 'GetSecretValue':
        return generate_secret_response(*args)
    if operation_name == 'Query':
        query_args = args[1]
        table_name = query_args['TableName']
        if table_name == 'Schema':
            return generate_schema_entry(*args)
        if table_name == 'Sensitives':
            return generate_sensitive_response(*args)
    if operation_name == 'UpdateItem':
        query_args = args[1]
        table_name = query_args['TableName']
        if table_name == 'Sensitives':
            return None
    if operation_name == 'DeleteTopic':
        return None
    if operation_name == 'Subscribe':
        return None
    raise NotImplementedError(f'cannot find an intercept command for {args}')
