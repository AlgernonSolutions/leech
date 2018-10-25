import os

import pytest
from datetime import datetime

from toll_booth.alg_obj.graph.ogm.regulators import PotentialEdge

table_name = os.getenv('TABLE_NAME', 'GraphObjects')
partition_key = os.getenv('PARTITION_KEY', 'identifier_stem')
sort_key = os.getenv('SORT_KEY', 'sid_value')

test_vertex_stem = 'algernon.test_vertex'
test_edge_stem = 'algernon.test_edge'
other_test_vertex_stem = 'algernon.other_test_vertex'
test_vertex_id_value = 0
test_vertex_properties = {'property_1': 'red', 'property_2': 3}
test_edge_properties = {
    'changed_target': {
        'change_date_utc': datetime.now().isoformat()
    }

}
test_vertex_id_value_field = 'property_1'
test_edge_id_value_field = 'edge_property_1'
test_vertex_object_type = 'TestVertex'
other_test_vertex_object_type = 'OtherTestVertex'
test_edge_object_type = '_changed_'
test_vertex_if_missing = 'pass'
test_vertex_internal_id = '1233'
other_vertex_internal_id = '3321'
test_edge_internal_id = '5678'

vertex = {
    partition_key: test_vertex_stem,
    sort_key: str(test_vertex_id_value),
    'id_value': test_vertex_id_value,
    'object_type': test_vertex_object_type,
    'if_missing': test_vertex_if_missing,
    'id_value_field': test_vertex_id_value_field,
    'is_edge': False,
    'completed': False,
    'internal_id': test_vertex_internal_id,
    'object_properties': test_vertex_properties
}

edge = {
    partition_key:  test_edge_stem,
    sort_key: str(test_vertex_id_value),
    'id_value': test_vertex_id_value,
    'id_value_field': test_edge_id_value_field,
    'object_type': test_edge_object_type,
    'object_properties': test_edge_properties,
    'internal_id': test_edge_internal_id,
    'from_object': test_vertex_internal_id,
    'to_object': other_vertex_internal_id
}

edge_data = edge.copy()
edge_data['changed_target'] = [{'change_date_utc': datetime.now()}]

other_vertex = vertex.copy()
other_vertex['object_type'] = test_vertex_object_type
other_vertex[partition_key] = other_test_vertex_stem

stub_key = {partition_key: 'stub', sort_key: '0'}
test_edge_key = {partition_key: test_edge_object_type, sort_key: test_edge_internal_id}


@pytest.fixture
def test_vertex():
    import boto3
    client = boto3.resource('dynamodb').Table(table_name)
    client.put_item(
        Item=vertex
    )
    yield vertex
    client.delete_item(
        Key={partition_key: vertex['identifier_stem'], sort_key: str(vertex['id_value'])}
    )


@pytest.fixture
def no_test_vertex():
    import boto3
    client = boto3.resource('dynamodb').Table(table_name)
    client.delete_item(
        Key={partition_key: test_vertex_stem, sort_key: str(test_vertex_id_value)}
    )
    yield vertex
    client.delete_item(
        Key={partition_key: test_vertex_stem, sort_key: str(test_vertex_id_value)}
    )


@pytest.fixture
def no_stub_vertex():
    import boto3
    client = boto3.resource('dynamodb').Table(table_name)
    client.delete_item(Key=stub_key)
    yield vertex
    client.delete_item(Key=stub_key)


@pytest.fixture
def test_vertex_range():
    import boto3
    client = boto3.resource('dynamodb').Table(table_name)
    test_vertexes = []
    for id_value in range(0, 5):
        range_vertex = vertex.copy()
        range_vertex['id_value'] = id_value
        range_vertex[sort_key] = str(id_value)
        range_vertex['internal_id'] = range_vertex['internal_id'] + str(id_value)
        test_vertexes.append(range_vertex)
    for entry in test_vertexes:
        client.put_item(
            Item=entry
        )
    yield test_vertexes
    for test_vertex in test_vertexes:
        client.delete_item(
            Key={partition_key: test_vertex['identifier_stem'], sort_key: str(test_vertex['id_value'])}
        )


@pytest.fixture
def no_test_vertexes():
    return test_vertex_stem


@pytest.fixture
def no_test_vertex_seed():
    import boto3
    client = boto3.resource('dynamodb').Table(table_name)
    client.delete_item(
        Key={partition_key: test_vertex_stem, sort_key: str(test_vertex_id_value)}
    )
    yield test_vertex_object_type, test_vertex_stem, test_vertex_id_value
    client.delete_item(
        Key={partition_key: test_vertex_stem, sort_key: str(test_vertex_id_value)}
    )


@pytest.fixture
def test_vertex_seed():
    import boto3
    client = boto3.resource('dynamodb').Table(table_name)
    client.put_item(
        Item={partition_key: test_vertex_stem, sort_key: str(test_vertex_id_value)}
    )
    yield test_vertex_object_type, test_vertex_stem, test_vertex_id_value, test_vertex_properties
    client.delete_item(
        Key={partition_key: test_vertex_stem, sort_key: str(test_vertex_id_value)}
    )


@pytest.fixture
def no_test_vertex_seeds():
    import boto3
    seed_range = range(10, 15)
    client = boto3.resource('dynamodb').Table(table_name)
    for id_value in seed_range:
        client.delete_item(Key={partition_key: test_vertex_stem, sort_key: str(id_value)})
    yield test_vertex_stem, seed_range, test_vertex_object_type
    for id_value in seed_range:
        client.delete_item(Key={partition_key: test_vertex_stem, sort_key: str(id_value)})


@pytest.fixture
def test_edge():
    return edge_data, vertex, other_vertex


@pytest.fixture
def no_test_edge():
    import boto3

    client = boto3.resource('dynamodb').Table(table_name)
    client.delete_item(Key=test_edge_key)
    potential_edge = PotentialEdge(
        test_edge_object_type, test_edge_internal_id, test_edge_properties,
        test_vertex_internal_id, other_vertex_internal_id
    )
    yield potential_edge
    client.delete_item(Key=test_edge_key)
