from decimal import Decimal

import boto3
import pytest
from botocore.exceptions import ClientError

from tests.units.test_data.dynamo_data import *

from toll_booth.alg_obj.aws.sapper.dynamo_driver import DynamoDriver, EmptyIndexException
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex, IdentifierStem, PotentialEdge, ObjectRegulator, \
    VertexRegulator
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEntry

blank_table_name = os.getenv('BLANK_TABLE_NAME')
filled_table_name = os.getenv('TABLE_NAME')


@pytest.mark.usefixtures('blank_table')
@pytest.mark.dynamo
class TestDynamoDriver:
    @pytest.mark.seed
    def test_vertex_seed_put(self, delete_vertex):
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        put_results = dynamo_driver.put_vertex_seed(identifier_stem=vertex_identifier_stem, id_value=vertex_id_value)
        assert put_results['ResponseMetadata']['HTTPStatusCode'] == 200
        client = boto3.resource('dynamodb').Table(blank_table_name)
        get_seeds = client.get_item(
            Key=vertex_key
        )
        fetched_seed = get_seeds['Item']
        assert fetched_seed['object_type'] == vertex_type
        assert str(fetched_seed['identifier_stem']) == str(vertex_identifier_stem)
        assert int(fetched_seed['id_value']) == vertex_id_value
        assert fetched_seed['completed'] is False
        assert fetched_seed['is_edge'] is False
        delete_vertex(blank_table_name, vertex_key)

    def test_vertex_get(self, put_vertex):
        put_vertex(blank_table_name, vertex, vertex_key)
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        get_test_results = dynamo_driver.get_vertex(vertex_identifier_stem, vertex_id_value)
        assert isinstance(get_test_results, PotentialVertex)
        assert get_test_results.id_value == vertex_id_value
        assert get_test_results.object_type == vertex_type
        assert str(get_test_results.identifier_stem) == str(vertex_identifier_stem)
        assert get_test_results.if_missing == 'pass'
        assert get_test_results.id_value_field == vertex_id_value_field
        assert get_test_results.object_properties == vertex_properties
        assert get_test_results.internal_id == vertex_internal_id

    def test_vertex_get_non_existent_vertex(self):
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        test_vertex = dynamo_driver.get_vertex(vertex_identifier_stem, vertex_id_value)
        assert test_vertex is None

    def test_write_vertex(self, delete_vertex):
        potential_vertex = PotentialVertex.from_json(vertex)
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        put_test_results = dynamo_driver.write_vertex(potential_vertex, 'testing')
        assert put_test_results['ResponseMetadata']['HTTPStatusCode'] == 200
        get_test_results = dynamo_driver.get_vertex(vertex_identifier_stem, vertex_id_value)
        assert isinstance(get_test_results, PotentialVertex)
        assert get_test_results.id_value == vertex_id_value
        assert get_test_results.object_type == vertex_type
        assert str(get_test_results.identifier_stem) == str(vertex_identifier_stem)
        assert get_test_results.if_missing == 'pass'
        assert get_test_results.id_value_field == vertex_id_value_field
        assert get_test_results.object_properties == vertex_properties
        assert get_test_results.internal_id == vertex_internal_id
        delete_vertex(blank_table_name, vertex_key)

    def test_write_edge(self, delete_test_edge):
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        potential_edge = PotentialEdge.from_json(edge)
        write_results = dynamo_driver.write_edge(potential_edge, 'testing')
        assert write_results['ResponseMetadata']['HTTPStatusCode'] == 200
        get_edge = dynamo_driver.get_edge(potential_edge.identifier_stem, edge_internal_id)
        assert isinstance(get_edge, PotentialEdge)
        assert get_edge.internal_id == potential_edge.internal_id
        delete_test_edge(blank_table_name, edge_key)

    def test_query_index_max(self, put_vertexes):
        put_vertexes(blank_table_name, vertex, id_range, sort_key, vertex_key)
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        index_max = dynamo_driver.query_index_value_max(vertex_identifier_stem, index_name)
        assert index_max == max(id_range)

    def test_query_index_max_no_entries_raises_empty_index_exception(self):
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        with pytest.raises(EmptyIndexException):
            dynamo_driver.query_index_value_max(vertex_identifier_stem)

    def test_vertex_double_write_yields_client_error(self, put_vertex):
        double_test_vertex = vertex.copy()
        double_test_vertex['testing_clear_time'] = Decimal(datetime.now().timestamp())
        put_vertex(blank_table_name, double_test_vertex, vertex_key)
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        test_vertex = PotentialVertex.from_json(vertex)
        with pytest.raises(ClientError) as e:
            dynamo_driver.write_vertex(test_vertex, 'testing')
        assert e.typename == 'ConditionalCheckFailedException'

    @pytest.mark.put_stub
    def test_stub_vertex_put(self, delete_vertex):
        stub_type = 'TestStub'
        rule_name = 'TestStubRule'
        source_internal_id = vertex_internal_id
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        dynamo_driver.add_stub_vertex(stub_type, vertex_properties, source_internal_id, rule_name)
        client = boto3.resource('dynamodb').Table(blank_table_name)
        seed_get = client.get_item(
            Key={partition_key: 'stub', sort_key: '0'}
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
        delete_vertex(blank_table_name, {partition_key: 'stub', sort_key: '0'})

    def test_mark_ids_as_working(self, delete_vertex):
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        working_ids, not_working_ids = dynamo_driver.mark_ids_as_working(vertex_identifier_stem, id_range, vertex_type)
        for id_value in id_range:
            assert id_value in not_working_ids
        assert working_ids == []
        working_ids, not_working_ids = dynamo_driver.mark_ids_as_working(vertex_identifier_stem, id_range, vertex_type)
        assert not_working_ids == []
        for id_value in id_range:
            assert id_value in working_ids
        for id_value in id_range:
            new_key = vertex_key.copy()
            new_key[sort_key] = str(id_value)
            delete_vertex(blank_table_name, new_key)

    def test_mark_object_as_blank(self, put_vertex):
        import boto3
        put_vertex(blank_table_name, vertex, vertex_key)
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        mark_results = dynamo_driver.mark_object_as_blank(vertex_identifier_stem, vertex_id_value)
        assert mark_results['ResponseMetadata']['HTTPStatusCode'] == 200
        client = boto3.resource('dynamodb').Table(blank_table_name)
        get_blank = client.get_item(Key=vertex_key)
        blank_vertex = get_blank['Item']
        assert blank_vertex['completed'] is True
        assert blank_vertex['disposition'] == 'blank'

    def test_mark_vertex_as_stage_cleared(self, put_vertex):
        import boto3
        import datetime

        put_vertex(blank_table_name, vertex, vertex_key)
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        test_mark = dynamo_driver.mark_object_as_stage_cleared(vertex_identifier_stem, vertex_id_value, 'testing')
        assert test_mark['ResponseMetadata']['HTTPStatusCode'] == 200
        client = boto3.resource('dynamodb').Table(blank_table_name)
        vertex_get = client.get_item(Key=vertex_key)
        test_vertex = vertex_get['Item']
        assert test_vertex['last_stage_seen'] == 'testing'
        last_seen_timestamp = test_vertex['last_seen_time']
        stage_clear_timestamp = test_vertex['testing_clear_time']
        assert last_seen_timestamp == stage_clear_timestamp
        test_timestamp = datetime.datetime.fromtimestamp(last_seen_timestamp)
        assert test_timestamp

    def test_mark_vertex_as_graphed(self, put_vertex):
        import boto3
        import datetime

        put_vertex(blank_table_name, vertex, vertex_key)
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        test_mark = dynamo_driver.mark_object_as_graphed(vertex_identifier_stem, vertex_id_value)
        assert test_mark['ResponseMetadata']['HTTPStatusCode'] == 200
        client = boto3.resource('dynamodb').Table(blank_table_name)
        vertex_get = client.get_item(Key=vertex_key)
        test_vertex = vertex_get['Item']
        assert test_vertex['last_stage_seen'] == 'graphing'
        last_seen_timestamp = test_vertex['last_seen_time']
        stage_clear_timestamp = test_vertex['graphing_clear_time']
        assert last_seen_timestamp == stage_clear_timestamp
        test_timestamp = datetime.datetime.fromtimestamp(last_seen_timestamp)
        assert test_timestamp
        assert test_vertex['disposition'] == 'processing'

    def test_find_potential_vertexes(self, put_vertexes):
        put_vertexes(blank_table_name, vertex, id_range, sort_key, vertex_key)
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        potential_vertexes = dynamo_driver.find_potential_vertexes(vertex_type, vertex_properties)
        for potential_vertex in potential_vertexes:
            assert potential_vertex.id_value in id_range
            for property_name, test_property in vertex_properties.items():
                assert potential_vertex.object_properties[property_name] == test_property

    def test_add_object_properties_to_populated_object_raises_client_error(self, put_vertex):
        put_vertex(blank_table_name, vertex, vertex_key)
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        with pytest.raises(ClientError) as e:
            dynamo_driver.add_object_properties(vertex_identifier_stem, vertex_id_value, vertex_properties)
        assert e.typename == 'ConditionalCheckFailedException'

    def test_add_object_properties(self, put_vertex):
        put_vertex(blank_table_name, seed_vertex, vertex_key)
        import boto3
        dynamo_driver = DynamoDriver(table_name=blank_table_name)
        add_results = dynamo_driver.add_object_properties(vertex_identifier_stem, vertex_id_value, vertex_properties)
        assert add_results['ResponseMetadata']['HTTPStatusCode'] == 200
        seed = boto3.resource('dynamodb').Table(blank_table_name).get_item(Key=vertex_key)
        assert seed["Item"]['object_properties'] == vertex_properties


@pytest.mark.edge
class TestPotentialEdge:
    def test_potential_edge_construction(self):
        potential_edge = PotentialEdge(edge_label, edge_internal_id, edge_properties, from_object_id, to_object_id)
        assert isinstance(potential_edge, PotentialEdge)

    def test_potential_edge_construction_from_json(self):
        potential_edge = PotentialEdge.from_json(edge)
        assert isinstance(potential_edge, PotentialEdge)

    def test_edge_identifier_stem_creation(self):
        potential_edge = PotentialEdge.from_json(edge)
        identifier_stem = potential_edge.identifier_stem
        assert isinstance(identifier_stem, IdentifierStem)


@pytest.mark.edge_regulator
class TestEdgeRegulator:
    def test_generate_potential_edge(self):
        source_potential_vertex = PotentialVertex.from_json(vertex)
        other_potential_vertex = PotentialVertex.from_json(other_vertex)
        regulator = ObjectRegulator.get_for_object_type(edge_label)
        potential_edge = regulator.generate_potential_edge(source_potential_vertex, other_potential_vertex, edge_extracted_data, False)
        assert isinstance(potential_edge, PotentialEdge)
        assert potential_edge.from_object == vertex_internal_id
        assert potential_edge.to_object == other_internal_id


@pytest.mark.vertex_regulator
class TestVertexRegulator:
    @pytest.mark.parametrize('vertex_data, object_type', [
        ({'id_type': 'Clients', 'id_value': 1941, 'id_name': 'client_id', 'id_source': 'MBI'}, 'ExternalId')
    ])
    def test_generate_potential_vertex(self, vertex_data, object_type):
        schema_entry = SchemaEntry.get(object_type)
        regulator = VertexRegulator(schema_entry)
        potential_vertex = regulator.create_potential_vertex(vertex_data)
        assert isinstance(potential_vertex, PotentialVertex)
