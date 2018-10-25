import boto3
import pytest
from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.aws_obj.dynamo_driver import DynamoDriver
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex


class TestDynamoDriver:
    def test_vertex_get(self, test_vertex):
        dynamo_driver = DynamoDriver()
        get_test_results = dynamo_driver.get_vertex(test_vertex['identifier_stem'], test_vertex['id_value'])
        assert isinstance(get_test_results, PotentialVertex)
        assert get_test_results.id_value == test_vertex['id_value']
        assert get_test_results.object_type == test_vertex['object_type']
        assert get_test_results.identifier_stem == test_vertex['identifier_stem']
        assert get_test_results.if_missing == test_vertex['if_missing']
        assert get_test_results.id_value_field == test_vertex['id_value_field']
        assert get_test_results.object_properties == test_vertex['object_properties']
        assert get_test_results.internal_id == test_vertex['internal_id']

    def test_vertex_get_non_existent_vertex(self, no_test_vertex):
        dynamo_driver = DynamoDriver()
        test_vertex = dynamo_driver.get_vertex(no_test_vertex['identifier_stem'], no_test_vertex['id_value'])
        assert test_vertex is None

    def test_vertex_put(self, no_test_vertex):
        potential_vertex = PotentialVertex.from_json(no_test_vertex)
        dynamo_driver = DynamoDriver()
        put_test_results = dynamo_driver.put_vertex(potential_vertex, 'testing')
        assert put_test_results['ResponseMetadata']['HTTPStatusCode'] == 200
        get_test_results = dynamo_driver.get_vertex(no_test_vertex['identifier_stem'], no_test_vertex['id_value'])
        assert isinstance(get_test_results, PotentialVertex)
        assert get_test_results.id_value == no_test_vertex['id_value']
        assert get_test_results.object_type == no_test_vertex['object_type']
        assert get_test_results.identifier_stem == no_test_vertex['identifier_stem']
        assert get_test_results.if_missing == no_test_vertex['if_missing']
        assert get_test_results.id_value_field == no_test_vertex['id_value_field']
        assert get_test_results.object_properties == no_test_vertex['object_properties']
        assert get_test_results.internal_id == no_test_vertex['internal_id']

    def test_query_index_max(self, test_vertex_range):
        identifier_stem = test_vertex_range[0]['identifier_stem']
        dynamo_driver = DynamoDriver()
        index_max = dynamo_driver.query_index_max(identifier_stem)
        test_range_max = max([x['id_value'] for x in test_vertex_range])
        assert index_max == test_range_max

    def test_query_index_max_no_entries(self, no_test_vertexes):
        dynamo_driver = DynamoDriver()
        index_max = dynamo_driver.query_index_max(no_test_vertexes)
        assert index_max == 0

    def test_vertex_double_put_yields_client_error(self, test_vertex):
        dynamo_driver = DynamoDriver()
        vertex = PotentialVertex.from_json(test_vertex)
        with pytest.raises(ClientError) as e:
            dynamo_driver.put_vertex(vertex, 'testing')
        assert e.typename == 'ConditionalCheckFailedException'

    def test_vertex_seed_put(self, test_vertex_seed):
        dynamo_driver = DynamoDriver()
        put_results = dynamo_driver.put_vertex_seed(*test_vertex_seed, stage_name='testing')
        assert put_results['ResponseMetadata']['HTTPStatusCode'] == 200
        client = boto3.resource('dynamodb').Table('Seeds')
        get_seeds = client.get_item(
            Key={'identifier_stem': test_vertex_seed[1], 'id_value': test_vertex_seed[2]}
        )
        fetched_seed = get_seeds['Item']
        assert fetched_seed['object_type'] == test_vertex_seed[0]
        assert fetched_seed['identifier_stem'] == test_vertex_seed[1]
        assert int(fetched_seed['id_value']) == test_vertex_seed[2]
        assert fetched_seed['completed'] is False
        assert fetched_seed['is_edge'] is False

    def test_stub_vertex_put(self, no_stub_vertex):
        stub_type = 'TestStub'
        rule_name = 'TestStubRule'
        stub_properties = no_stub_vertex['object_properties']
        source_internal_id = no_stub_vertex['internal_id']
        dynamo_driver = DynamoDriver()
        dynamo_driver.add_stub_vertex(stub_type, stub_properties, source_internal_id, rule_name)
        client = boto3.resource('dynamodb').Table('Seeds')
        seed_get = client.get_item(
            Key={'identifier_stem': 'stub', 'id_value': 0}
        )
        stubs = seed_get['Item']
        assert 'TestStub' in stubs
        returned_stub_properties = stubs['TestStub'][0]['object_properties']
        returned_stub_properties['property_2'] = int(returned_stub_properties['property_2'])
        assert stubs['TestStub'][0] == {
            'source_internal_id': source_internal_id,
            'rule_name': rule_name,
            'object_properties': returned_stub_properties
        }

    def test_mark_ids_as_working(self, no_test_vertex_seeds):
        identifier_stem = no_test_vertex_seeds[0]
        id_range = no_test_vertex_seeds[1]
        object_type = no_test_vertex_seeds[2]
        dynamo_driver = DynamoDriver()
        working_ids, not_working_ids = dynamo_driver.mark_ids_as_working(identifier_stem, id_range, object_type)
        for id_value in id_range:
            assert id_value in not_working_ids
        assert working_ids == []
        working_ids, not_working_ids = dynamo_driver.mark_ids_as_working(identifier_stem, id_range, object_type)
        assert not_working_ids == []
        for id_value in id_range:
            assert id_value in working_ids

    def test_mark_object_as_blank(self, test_vertex):
        import boto3
        dynamo_driver = DynamoDriver()
        identifier_stem = test_vertex['identifier_stem']
        id_value = test_vertex['id_value']
        mark_results = dynamo_driver.mark_object_as_blank(identifier_stem, id_value)
        assert mark_results['ResponseMetadata']['HTTPStatusCode'] == 200
        client = boto3.resource('dynamodb').Table('Seeds')
        get_blank = client.get_item(Key={'identifier_stem': identifier_stem, 'id_value': id_value})
        blank_vertex = get_blank['Item']
        assert blank_vertex['completed'] is True
        assert blank_vertex['disposition'] == 'blank'
