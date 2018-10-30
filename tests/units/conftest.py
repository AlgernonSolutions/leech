import datetime
import os
from decimal import Decimal

import pytest

from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex, MissingObjectProperty


@pytest.fixture(scope='module')
def blank_table():
    indexed_field = os.getenv('indexed_field', 'id_value')
    table_name = os.getenv('table_name')
    partition_key = os.getenv('partition_key')
    sort_key = os.getenv('sort_key')
    index_name = os.getenv('index_name')
    import boto3

    client = boto3.client('dynamodb')
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

    yield

    client.delete_table(TableName=table_name)


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


credible_ws_extractor = 'CredibleWebServiceExtractor'
credible_ws_extractor_function = 'leech-extract-crediblews'
ext_id_identifier_stem = '#vertex#ExternalId#{\"id_source\": \"MBI\", \"id_type\": \"Clients\", \"id_name\": \"client_id\"}#'
change_identifier_stem = '#vertex#Change#{\"id_source\": \"MBI\", \"id_type\": \"ChangeLogDetail\", \"id_name\": \"changelogdetail_id\"}#'
ext_id_id_value = 1941
change_id_value = 1230
change_internal_id = '7b4043d1fe270c1ed1f032f8ec31e899'
ext_id_extracted_data = {"source": {"id_value": ext_id_id_value, "id_type": "Clients", "id_name": "client_id", "id_source": "MBI"}}
change_extracted_data = {'source': {'id_name': 'changelogdetail_id', 'id_type': 'ChangeLogDetail', 'id_source': 'MBI', 'detail_id': 1230, 'changelog_id': 177, 'data_dict_id': 49, 'detail_one_value': '', 'detail_one': '', 'detail_two': '301-537-8676'}, 'changed_target': [{'change_date_utc': datetime.datetime(2014, 7, 29, 14, 44, 44, 367000), 'client_id': '', 'clientvisit_id': '', 'emp_id': 3889, 'record_id': '', 'record_type': '', 'primarykey_name': ''}]}
change_properties = {'detail_id': Decimal('1230'), 'id_source': 'MBI', 'changelog_id': Decimal('177'), 'data_dict_id': Decimal('49'), 'detail_one': 'e7b0192b71294db66f1ac4e0a9b36bff', 'detail_one_value': 'e7b0192b71294db66f1ac4e0a9b36bff', 'detail_two': 'e7b0192b71294db66f1ac4e0a9b36bff'}
source_vertex = PotentialVertex('Change', change_internal_id, change_properties, None, change_identifier_stem, change_id_value, 'detail_id')

first_potential_internal_id = 'ff9ccecb77051747a9ff6cc4169de27d'
first_potential_vertex_properties = {'id_value': Decimal('3889'), 'id_source': 'MBI', 'id_type': 'Employees', 'id_name': 'emp_id'}
first_potential_identifier_stem = '#vertex#ExternalId#{"id_source": "MBI", "id_type": "Employees", "id_name": "emp_id"}#'
first_potential_vertex = PotentialVertex('ExternalId', first_potential_internal_id, first_potential_vertex_properties, 'create', first_potential_identifier_stem, 3889, 'id_value')

second_potential_internal_id = 'c5bf540a3fb43491ad13e446cb9c3757'
second_potential_vertex_properties = {'changelog_id': Decimal('177'), 'change_description': MissingObjectProperty(), 'change_date':MissingObjectProperty(), 'change_date_utc': MissingObjectProperty(), 'id_source': 'MBI'}
second_potential_identifier_stem = ['id_source', 'id_type', 'id_name']
second_potential_vertex = PotentialVertex('ExternalId', second_potential_internal_id, second_potential_vertex_properties, 'stub', second_potential_identifier_stem, 3889, 'id_value')

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
])
def transform_order(request):
    from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEntry
    from toll_booth.alg_obj.forge.comms.orders import TransformObjectOrder

    params = request.param
    schema_entry = SchemaEntry.get(params[0])
    return TransformObjectOrder(params[1], params[2], params[3], schema_entry)


@pytest.fixture(params=[
    (source_vertex, first_potential_vertex_properties, None, change_extracted_data),
    (source_vertex, second_potential_vertex, None, change_extracted_data),
])
def assimilate_order(request):
    from toll_booth.alg_obj.forge.comms.orders import AssimilateObjectOrder

    params = request.param
    return AssimilateObjectOrder(params[0], params[1], params[2], params[3])
