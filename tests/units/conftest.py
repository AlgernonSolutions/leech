import os
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from mock import MagicMock

from tests.units.test_data import patches
from tests.units.test_data.actor_data import *
from tests.units.test_data.assimilation_orders import first_identifiable_assimilation_order, \
    first_stub_assimilate_order, second_stub_assimilate_order
from tests.units.test_data.dynamo_stream_events import *
from tests.units.test_data.potential_vertexes import *


@pytest.fixture(scope='session')
def blank_table():
    table_name = os.getenv('blank_table_name', 'TestObjects')
    import boto3
    client = boto3.client('dynamodb')

    try:
        _create_table(client, table_name)
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceInUseException':
            raise e
        try:
            client.delete_table(TableName=table_name)
        except ClientError:
            pass
        waiter = client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name, WaiterConfig={'Delay': 10})
        _create_table(client, table_name)

    yield

    client.delete_table(TableName=table_name)


def _create_table(client, table_name):
    indexed_field = os.getenv('indexed_field', 'id_value')
    partition_key = os.getenv('partition_key')
    sort_key = os.getenv('sort_key')
    index_name = os.getenv('index_name')

    client.create_table(
        AttributeDefinitions=[
            {'AttributeName': partition_key, 'AttributeType': 'S'},
            {'AttributeName': sort_key, 'AttributeType': 'S'},
            {'AttributeName': indexed_field, 'AttributeType': 'N'},
        ],
        TableName=table_name,
        KeySchema=[
            {'AttributeName': partition_key, 'KeyType': 'HASH'},
            {'AttributeName': sort_key, 'KeyType': 'RANGE'}
        ],
        GlobalSecondaryIndexes=[{
            'IndexName': index_name,
            'KeySchema': [
                {'AttributeName': partition_key, 'KeyType': 'HASH'},
                {'AttributeName': indexed_field, 'KeyType': 'RANGE'}
            ],
            'Projection': {'ProjectionType': 'KEYS_ONLY'},
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 3,
                'WriteCapacityUnits': 3
            }
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 3,
            'WriteCapacityUnits': 3
        }
    )
    waiter = client.get_waiter('table_exists')
    waiter.wait(TableName=table_name, WaiterConfig={'Delay': 10})


@pytest.fixture
def put_vertexes():
    import boto3
    vertex_keys = []
    table_names = []

    def _put_vertexes(table_name, vertex, id_range, sort_key, vertex_key):
        import boto3

        client = boto3.resource('dynamodb').Table(table_name)
        table_names.append(table_name)
        for id_value in id_range:
            new_key = vertex_key.copy()
            new_vertex = vertex.copy()
            new_vertex['id_value'] = id_value
            new_vertex[sort_key] = str(id_value)
            new_key[sort_key] = str(id_value)
            client.put_item(Item=new_vertex)
            vertex_keys.append(new_key)

    yield _put_vertexes

    for name in table_names:
        test_client = boto3.resource('dynamodb').Table(name)
        for key in vertex_keys:
            test_client.delete_item(Key=key)


@pytest.fixture
def put_vertex():
    import boto3
    vertex_keys = []
    table_names = []

    def _put_vertex(table_name, vertex, vertex_key):
        import boto3

        table_names.append(table_name)
        vertex_keys.append(vertex_key)
        client = boto3.resource('dynamodb').Table(table_name)
        client.put_item(Item=vertex)

    yield _put_vertex

    for name in table_names:
        test_client = boto3.resource('dynamodb').Table(name)
        for key in vertex_keys:
            test_client.delete_item(Key=key)


@pytest.fixture
def delete_test_edge():
    import boto3

    def _delete_edge(table_name, edge_key):
        client = boto3.resource('dynamodb').Table(table_name)
        client.delete_item(Key=edge_key)

    yield _delete_edge


@pytest.fixture
def delete_vertex():
    def _delete_vertex(table_name, vertex_key):
        import boto3
        client = boto3.resource('dynamodb').Table(table_name)
        client.delete_item(Key=vertex_key)

    yield _delete_vertex


@pytest.fixture(params=[
    ext_id_identifier_stem,
    change_identifier_stem
])
def identifier_stem(request):
    return request.param


@pytest.fixture(params=[
    ('ExternalId', ext_id_identifier_stem, ext_id_id_value, credible_ws_extractor, credible_ws_extractor_function),
    ('Change', change_identifier_stem, change_id_value, credible_ws_extractor, credible_ws_extractor_function)
])
def extraction_order(request):
    from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEntry
    from toll_booth.alg_obj.forge.comms.orders import ExtractObjectOrder
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    params = request.param
    identifier_stem = IdentifierStem.from_raw(params[1])
    schema_entry = SchemaEntry.get(params[0])
    extraction_properties = identifier_stem.for_extractor
    schema_extraction_properties = schema_entry.extract[params[3]]
    extraction_properties.update(schema_extraction_properties.extraction_properties)
    id_value = params[2]
    extractor_function_name = params[4]
    return ExtractObjectOrder(identifier_stem, id_value, extractor_function_name, extraction_properties, schema_entry)


@pytest.fixture(params=[
    ('ExternalId', ext_id_identifier_stem, ext_id_id_value, ext_id_extracted_data),
    ('Change', change_identifier_stem, change_id_value, change_extracted_data),
    ('Change', change_identifier_stem, second_change_id_value, second_change_extracted_data),
])
def transform_order(request):
    from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEntry
    from toll_booth.alg_obj.forge.comms.orders import TransformObjectOrder

    params = request.param
    schema_entry = SchemaEntry.get(params[0])
    return TransformObjectOrder(params[1], params[2], params[3], schema_entry)


@pytest.fixture(params=[
    first_identifiable_assimilation_order,
])
def identifiable_assimilate_order(request):
    return request.param


@pytest.fixture(params=[
    first_stub_assimilate_order,
    second_stub_assimilate_order
])
def stubbed_assimilate_order(request):
    return request.param


@pytest.fixture(params=[
    ('MBI', 'Employees', 'emp_id', 'vertex', 'ExternalId'),
    ('MBI', 'Clients', 'client_id', 'vertex', 'ExternalId'),
    ('MBI', 'ClientVisit', 'clientvisit_id', 'vertex', 'ExternalId'),
    ('MBI', 'ChangeLogDetail', 'changelogdetail_id', 'vertex', 'Change')
])
def credible_ws_payload(request):
    params = request.param
    return {
        'step_name': 'index_query',
        'step_args': {
            'id_source': params[0],
            'id_type': params[1],
            'id_name': params[2],
            'graph_type': params[3],
            'object_type': params[4]
        }
    }


@pytest.fixture(params=[
    insert_event, remove_event, graphing_event
])
def dynamo_stream_event(request):
    return request.param


@pytest.fixture(params=[
    ('1002', '#vertex#ExternalId#{"id_source": "MBI", "id_type": "Clients", "id_name": "client_id"}#')
])
def load_order(request):
    params = request.param
    return {
        'task_name': 'load',
        'task_args': {
            'keys': {
                'sid_value': {'S': params[0]},
                'identifier_stem': {
                    'S': params[1]
                }
            },
            'table_name': 'GraphObjects'
        }
    }


@pytest.fixture(params=[
    external_id_potential_vertex,
    change_potential_vertex,
    changelog_potential_vertex
])
def potential_vertex(request):
    return request.param


@pytest.fixture
def mock_context():
    context = MagicMock(name='context')
    context.function_name = 'test_function'
    context.invoked_function_arn = 'test_function_arn'
    context.aws_request_id = '12344_request_id'
    return context


@pytest.fixture
def silence_x_ray():
    patch(patches.x_ray_patch_begin).start()
    patch(patches.x_ray_patch_end).start()
    yield
    patch(patches.x_ray_patch_begin).start()
    patch(patches.x_ray_patch_end).start()


@pytest.fixture
def mock_neptune():
    fish_sticks = patch(patches.neptune_patch).start()
    yield fish_sticks
    patch(patches.neptune_patch).start()
