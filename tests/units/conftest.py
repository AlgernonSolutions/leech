import os

import pytest


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
ext_id_id_value = 1941
ext_id_extracted_data = {"source": {"id_value": ext_id_id_value, "id_type": "Clients", "id_name": "client_id", "id_source": "MBI"}}


@pytest.fixture(params=[
    ext_id_identifier_stem
])
def identifier_stem(request):
    return request.param[0]


@pytest.fixture(params=[
    ('ExternalId', ext_id_identifier_stem, ext_id_id_value, credible_ws_extractor, credible_ws_extractor_function)
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
    ('ExternalId', ext_id_identifier_stem, ext_id_id_value, ext_id_extracted_data)
])
def transform_order(request):
    from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEntry
    from toll_booth.alg_obj.forge.comms.orders import TransformObjectOrder

    params = request.param
    schema_entry = SchemaEntry.get(params[0])
    return TransformObjectOrder(params[1], params[2], params[3], schema_entry)
