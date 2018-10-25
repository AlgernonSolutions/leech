import pytest

test_vertex_stem = 'algernon.test_vertex'
other_test_vertex_stem = 'algernon.other_test_vertex'
text_vertex_id_value = 0
test_vertex_properties = {'property_1': 'red', 'property_2': 3}
test_vertex_id_value_field = 'property_1'
test_vertex_object_type = 'TestVertex'
other_test_vertex_object_type = 'OtherTestVertex'
test_vertex_if_missing = 'pass'
test_vertex_internal_id = '1233'

vertex = {
    'identifier_stem': test_vertex_stem,
    'id_value': text_vertex_id_value,
    'object_type': test_vertex_object_type,
    'if_missing': test_vertex_if_missing,
    'id_value_field': test_vertex_id_value_field,
    'is_edge': False,
    'completed': False,
    'internal_id': test_vertex_internal_id,
    'object_properties': test_vertex_properties
}

other_vertex = vertex.copy()
other_vertex['object_type'] = test_vertex_object_type
other_vertex['identifier_stem'] = other_test_vertex_stem

stub_key = {'identifier_stem': 'stub', 'id_value': 0}


@pytest.fixture
def test_vertex():
    import boto3
    client = boto3.resource('dynamodb').Table('Seeds')
    client.put_item(
        Item=vertex
    )
    yield vertex
    client.delete_item(
        Key={'identifier_stem': vertex['identifier_stem'], 'id_value': vertex['id_value']}
    )


@pytest.fixture
def no_test_vertex():
    import boto3
    client = boto3.resource('dynamodb').Table('Seeds')
    client.delete_item(
        Key={'identifier_stem': test_vertex_stem, 'id_value': text_vertex_id_value}
    )
    yield vertex
    client.delete_item(
        Key={'identifier_stem': test_vertex_stem, 'id_value': text_vertex_id_value}
    )


@pytest.fixture
def no_stub_vertex():
    import boto3
    client = boto3.resource('dynamodb').Table('Seeds')
    client.delete_item(Key=stub_key)
    yield vertex
    client.delete_item(Key=stub_key)


@pytest.fixture
def test_vertex_range():
    import boto3
    client = boto3.resource('dynamodb').Table('Seeds')
    test_vertexes = []
    for id_value in range(0, 5):
        range_vertex = vertex.copy()
        range_vertex['id_value'] = id_value
        range_vertex['internal_id'] = range_vertex['internal_id'] + str(id_value)
        test_vertexes.append(range_vertex)
    for entry in test_vertexes:
        client.put_item(
            Item=entry
        )
    yield test_vertexes
    for test_vertex in test_vertexes:
        client.delete_item(
            Key={'identifier_stem': test_vertex['identifier_stem'], 'id_value': test_vertex['id_value']}
        )


@pytest.fixture
def no_test_vertexes():
    return test_vertex_stem


@pytest.fixture
def test_vertex_seed():
    return test_vertex_object_type, test_vertex_stem, text_vertex_id_value


@pytest.fixture
def no_test_vertex_seeds():
    import boto3
    seed_range = range(10, 15)
    client = boto3.resource('dynamodb').Table('Seeds')
    for id_value in seed_range:
        client.delete_item(Key={'identifier_stem': test_vertex_stem, 'id_value': id_value})
    yield test_vertex_stem, seed_range, test_vertex_object_type
    for id_value in seed_range:
        client.delete_item(Key={'identifier_stem': test_vertex_stem, 'id_value': id_value})
