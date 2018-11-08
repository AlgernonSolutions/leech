from decimal import Decimal

import pytest
from mock import patch

from tests.units.test_data import patches
from tests.units.test_data.assimilation_orders.first_identifiable_assimilation_order import \
    rule_entry as first_rule_entry
from tests.units.test_data.data_setup.boto import intercept
from tests.units.test_data.dynamo_data import *
from tests.units.test_data.potential_vertexes import *
from tests.units.test_data.schema_generator import get_schema_entry
from toll_booth.alg_obj.aws.sapper.dynamo_driver import LeechDriver

blank_table_name = os.getenv('BLANK_TABLE_NAME')

expected_seed_keys = [
    'identifier_stem', 'sid_value', 'disposition', 'last_stage_seen',
    'object_type', 'last_time_seen', 'last_stage_seen', 'completed', 'progress'
]


@pytest.mark.local_dynamo
class TestDynamoDriver:
    @pytest.mark.mark_ids
    def test_mark_ids_as_working(self, test_working_ids, dynamo_test_environment):
        function_name = 'mark_ids_as_working'
        dynamo_driver = LeechDriver(table_name=blank_table_name)
        test_id_range = test_working_ids[1]
        test_identifier_stem = test_working_ids[0]
        results = dynamo_driver.mark_ids_as_working(test_id_range, identifier_stem=test_identifier_stem)
        assert results == ([], list(test_id_range))
        assert dynamo_test_environment.called is True
        assert dynamo_test_environment.call_count == len(test_id_range)
        for boto_call in dynamo_test_environment.call_args_list:
            dynamo_commands = boto_call[0]
            dynamo_args = dynamo_commands[0]
            dynamo_kwargs = dynamo_commands[1]
            assert dynamo_args == 'UpdateItem'
            assert dynamo_kwargs['Key']['identifier_stem'] == str(test_identifier_stem)
            assert int(dynamo_kwargs['Key']['sid_value']) in test_id_range
            update_expression = dynamo_kwargs['UpdateExpression']
            update_names = dynamo_kwargs['ExpressionAttributeNames']
            update_values = dynamo_kwargs['ExpressionAttributeValues']
            self._assert_update_expression_creation(function_name, update_expression)
            self._assert_attribute_names_creation(function_name, update_names)
            self._assert_attribute_values_creation(function_name, update_values, id_value_range=test_id_range, object_type=test_identifier_stem.object_type)

    @pytest.mark.mark_blank
    def test_mark_object_as_blank(self, test_id, dynamo_test_environment):
        function_name = 'mark_object_as_blank'
        dynamo_driver = LeechDriver(table_name=blank_table_name)
        test_identifier_stem = test_id[0]
        test_id_value = test_id[1]
        dynamo_driver.mark_object_as_blank(identifier_stem=test_identifier_stem, id_value=test_id_value)
        self._assert_dynamo_call(function_name, test_id_value, test_identifier_stem, dynamo_test_environment, stage_name='extraction')

    @pytest.mark.mark_graphed
    def test_mark_object_as_graphed(self, test_id, dynamo_test_environment):
        function_name = 'mark_object_as_graphed'
        test_identifier_stem = test_id[0]
        test_id_value = test_id[1]
        dynamo_driver = LeechDriver(table_name=blank_table_name)
        dynamo_driver.mark_object_as_graphed(identifier_stem=test_identifier_stem, id_value=test_id_value)
        self._assert_dynamo_call(function_name, test_id_value, test_identifier_stem, dynamo_test_environment, stage_name='graphing')

    @pytest.mark.mark_transform
    def test_set_transform_results(self, test_transform_results, dynamo_test_environment):
        function_name = 'set_transform_results'
        dynamo_driver = LeechDriver(table_name=blank_table_name)
        test_source_vertex = test_transform_results[0]
        test_potentials = test_transform_results[1]
        test_id_value = test_source_vertex.id_value
        test_identifier_stem = test_source_vertex.identifier_stem
        test_internal_id = test_source_vertex.internal_id
        dynamo_driver.set_transform_results(
            test_source_vertex, test_potentials,
            identifier_stem=test_identifier_stem,
            id_value=test_id_value)
        disposition = 'working'
        if not test_potentials:
            disposition = 'graphing'
        test_args = (function_name, test_id_value, test_identifier_stem, dynamo_test_environment)
        test_kwargs = {
            'stage_name': 'transformation',
            'disposition': disposition,
            'internal_id': test_internal_id
        }
        self._assert_dynamo_call(*test_args, **test_kwargs)
        attribute_values = dynamo_test_environment.call_args_list[0][0][1]['ExpressionAttributeValues']
        self._assert_object_properties_creation(test_source_vertex.object_type, attribute_values[':v'])
        self._assert_potentials_creation(attribute_values[':ps'], test_source_vertex)

    @pytest.mark.mark_assimilation
    def test_set_assimilation_results(self, dynamo_test_environment):
        dynamo_driver = LeechDriver(table_name=blank_table_name)
        assimilation_results = [
            {'edge': changed_edge, 'vertex': external_id_potential_vertex}
        ]
        dynamo_driver.set_assimilation_results(
            '_changed_', assimilation_results,
            identifier_stem=change_potential_vertex.identifier_stem,
            id_value=change_potential_vertex.id_value
        )
        assert dynamo_test_environment.called is True
        boto_args = dynamo_test_environment.call_args[0][0]
        boto_kwargs = dynamo_test_environment.call_args[0][1]
        assert boto_args == 'UpdateItem'
        assert boto_kwargs['Key'] == {
            'identifier_stem': '#vertex#Change#{"id_source": "MBI", "id_type": "ChangeLogDetail", "id_name": "changelogdetail_id"}#',
            'sid_value': str(1230)}
        update_expression = boto_kwargs['UpdateExpression']
        for entry in ['#lss', '#lts', '#p', '#iv', '#a']:
            assert entry in update_expression
        for entry in [':s', ':t', ':iv', ':a']:
            assert entry in update_expression
        update_values = boto_kwargs['ExpressionAttributeValues']
        assert update_values[':s'] == 'assimilation'
        for entry in update_values[':iv']:
            test_edge = entry['edge']
            test_vertex = entry['vertex']
            for key_value in ['identifier_stem', 'sid_value', 'internal_id', 'object_properties', 'from_object',
                              'to_object']:
                assert key_value in test_edge
            for key_value in ['identifier_stem', 'sid_value', 'id_value', 'internal_id', 'object_properties']:
                assert key_value in test_vertex

    @classmethod
    def _assert_dynamo_call(cls, function_name, test_id_value, test_identifier_stem, mock_boto, **kwargs):
        mock_boto.assert_called_once()
        boto_args = mock_boto.call_args[0][0]
        boto_kwargs = mock_boto.call_args[0][1]
        assert boto_args == 'UpdateItem'
        assert boto_kwargs['Key'] == {
            'identifier_stem': str(test_identifier_stem),
            'sid_value': str(test_id_value)
        }
        cls._assert_update_expression_creation(function_name, boto_kwargs['UpdateExpression'])
        cls._assert_attribute_names_creation(function_name, boto_kwargs['ExpressionAttributeNames'], **kwargs)
        cls._assert_attribute_values_creation(function_name, boto_kwargs['ExpressionAttributeValues'], **kwargs)

    @classmethod
    def _assert_update_expression_creation(cls, function_name, update_expression):
        common_expressions = ['#lts=:t', '#lss=:s']
        expected_expressions = {
            'mark_ids_as_working': ['#c=:c', '#p=:p', '#d=:d', '#ot=:ot', '#id=:id'],
            'mark_object_as_blank': ['#c=:c', '#p.#s=:p', '#d=:d'],
            'mark_object_as_graphed': ['#d=:d'],
            'set_transform_results': ['#ps=:ps', '#o=:v', '#p.#s=:p', '#d=:d']
        }
        function_expressions = expected_expressions[function_name]
        function_expressions.extend(common_expressions)
        for expression in function_expressions:
            assert expression in update_expression

    @classmethod
    def _assert_attribute_names_creation(cls, function_name, attribute_names, **kwargs):
        common_names = {
            '#lss': 'last_stage_seen', '#lts': 'last_time_seen', '#p': 'progress'
        }
        expected_names = {
            'mark_ids_as_working': {
                '#d': 'disposition', '#c': 'completed', '#id': 'id_value', '#ot': 'object_type'
            },
            'mark_object_as_blank': {
                '#d': 'disposition', '#c': 'completed', '#s': kwargs.get('stage_name')
            },
            'mark_object_as_graphed': {
                '#d': 'disposition', '#s': kwargs.get('stage_name')
            },
            'set_transform_results': {
                '#i': 'internal_id', '#o': 'object_properties', '#d': 'disposition', '#ps': 'potentials', '#s': kwargs.get('stage_name')
            }
        }
        function_expressions = common_names.copy()
        function_expressions.update(expected_names[function_name])
        for name_id, name_value in function_expressions.items():
            assert name_id in attribute_names
            assert attribute_names[name_id] == name_value
        for name_id, name_value in attribute_names.items():
            assert name_id in function_expressions
            assert function_expressions[name_id] == name_value

    @classmethod
    def _assert_attribute_values_creation(cls, function_name, attribute_values, **kwargs):
        now_timestamp = attribute_values[':t']
        progress = attribute_values[':p']
        if function_name == 'mark_ids_as_working':
            assert isinstance(progress, dict)
            assert len(progress) == 1
            assert 'monitoring' in progress
            assert isinstance(progress['monitoring'], Decimal)
            assert ':id' in attribute_values
            assert attribute_values[':id'] in kwargs['id_value_range']
        else:
            assert isinstance(progress, Decimal)
        assert isinstance(now_timestamp, Decimal)
        expected_values = {
            'mark_ids_as_working': {':c': False, ':d': 'working', ':s': 'monitoring', ':ot': kwargs.get('object_type')},
            'mark_object_as_blank': {':c': True, ':d': 'blank', ':s': 'extraction'},
            'mark_object_as_graphed': {':d': 'processing', ':s': 'graphing'},
            'set_transform_results': {':i': kwargs.get('internal_id'), ':d': kwargs.get('disposition'), ':s': kwargs.get('stage_name')}
        }
        function_values = expected_values[function_name]
        for value_name, value in function_values.items():
            assert value_name in attribute_values
            assert attribute_values[value_name] == value
        for value_name, value in attribute_values.items():
            if value_name in [':t', ':p', ':id', ':v', ':ps']:
                continue
            assert value_name in function_values
            assert function_values[value_name] == value

    @classmethod
    def _assert_object_properties_creation(cls, object_type, object_properties):
        object_schema = get_schema_entry(object_type)
        for property_name, entry_property in object_schema.entry_properties.items():
            assert property_name in object_properties
            property_value = object_properties[property_name]
            cls._assert_object_property_data_type(property_value, entry_property.property_data_type)

    @classmethod
    def _assert_object_property_data_type(cls, property_value, property_data_type):
        if property_value is None:
            return
        if property_data_type == 'String':
            assert isinstance(property_value, str)
            return
        if property_data_type == 'Number':
            assert isinstance(property_value, Decimal)
            return
        if property_data_type == 'DateTime':
            datetime.strptime(property_value, '%Y-%m-%dT%H:%M:%S%z')
            return
        raise NotImplementedError('could not test object property for data_type: %s' % property_data_type)

    @classmethod
    def _assert_potentials_creation(cls, potentials, test_source_vertex):
        schema_entry = get_schema_entry(test_source_vertex.object_type)
        schema_linking_rules = schema_entry.rules.linking_rules
        rules = []
        for linking_rule in schema_linking_rules:
            rules.extend(linking_rule.rules)
        for ruled_edge_label, potential in potentials.items():
            rule_entry = potential['rule_entry']
            potential_vertex = potential['potential_vertex']
            print()
