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
