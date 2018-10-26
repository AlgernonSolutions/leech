import pytest


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
            vertex['id_value'] = id_value
            vertex_key[sort_key] = str(id_value)
            client.put_item(Item=vertex)
            vertex_keys.append(vertex_key)

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
def delete_test_edge(table_name, edge_key):
    import boto3

    client = boto3.resource('dynamodb').Table(table_name)
    client.delete_item(Key=edge_key)
    yield
    client.delete_item(Key=edge_key)
