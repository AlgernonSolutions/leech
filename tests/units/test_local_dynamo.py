from decimal import Decimal

import pytest
from mock import patch

from tests.steps.actor_setup import patches
from tests.steps.outside_setup.boto import intercept
from tests.units.test_data.assimilation_orders.first_identifiable_assimilation_order import \
    rule_entry as first_rule_entry
from tests.units.test_data.dynamo_data import *
from tests.units.test_data.potential_vertexes import *
from toll_booth.alg_obj.aws.sapper.dynamo_driver import LeechDriver

blank_table_name = os.getenv('BLANK_TABLE_NAME')

expected_seed_keys = [
    'identifier_stem', 'sid_value', 'disposition', 'last_stage_seen',
    'object_type', 'last_time_seen', 'last_stage_seen', 'completed', 'progress'
]


@pytest.mark.local_dynamo
class TestDynamoDriver:
    @pytest.mark.put_seed
    def test_seed_put(self):
        with patch(patches.boto_patch, side_effect=intercept) as mock_boto:
            dynamo_driver = LeechDriver(table_name=blank_table_name)
            dynamo_driver.put_vertex_seed(identifier_stem=vertex_identifier_stem, id_value=vertex_id_value)
            assert mock_boto.called is True
            dynamo_commands = mock_boto.call_args[0]
            dynamo_args = dynamo_commands[0]
            dynamo_kwargs = dynamo_commands[1]
            assert dynamo_args == 'PutItem'
            assert dynamo_kwargs['TableName'] == blank_table_name
            dynamo_item = dynamo_kwargs['Item']

            for expected_key in expected_seed_keys:
                assert expected_key in dynamo_item
            dynamo_condition = dynamo_kwargs['ConditionExpression']
            assert dynamo_condition

    @pytest.mark.mark_ids
    def test_mark_ids_as_working(self):
        with patch(patches.boto_patch, side_effect=intercept) as mock_boto:
            dynamo_driver = LeechDriver(table_name=blank_table_name)
            results = dynamo_driver.mark_ids_as_working(id_range, identifier_stem=vertex_identifier_stem, id_value=vertex_id_value)
            assert results == ([], list(id_range))
            assert mock_boto.called is True
            assert mock_boto.call_count == len(id_range)
            for boto_call in mock_boto.call_args_list:
                call_args = boto_call[0]
                assert call_args[0] == 'PutItem'
                for expected_key in expected_seed_keys:
                    assert expected_key in call_args[1]['Item']

    @pytest.mark.mark_blank
    def test_mark_object_as_blank(self):
        with patch(patches.boto_patch, side_effect=intercept) as mock_boto:
            dynamo_driver = LeechDriver(table_name=blank_table_name)
            dynamo_driver.mark_object_as_blank(identifier_stem=vertex_identifier_stem, id_value=vertex_id_value)
            assert mock_boto.called is True
            boto_args = mock_boto.call_args[0][0]
            boto_kwargs = mock_boto.call_args[0][1]
            assert boto_args == 'UpdateItem'
            assert boto_kwargs['Key'] == {'identifier_stem': str(vertex_identifier_stem), 'sid_value': str(vertex_id_value)}
            update_expression = boto_kwargs['UpdateExpression']
            assert '#c=:c' in update_expression
            assert '#p=:t' in update_expression
            assert '#d=:d' in update_expression
            assert '#lts=:t' in update_expression
            assert '#lss=:s' in update_expression
            update_values = boto_kwargs['ExpressionAttributeValues']
            update_names = boto_kwargs['ExpressionAttributeNames']
            assert update_names['#d'] == 'disposition'
            assert update_names['#lss'] == 'last_stage_seen'
            assert update_names['#lts'] == 'last_time_seen'
            assert update_names['#c'] == 'completed'
            assert update_values[':c'] is True
            assert update_values[':d'] == 'blank'
            assert update_values[':s'] == 'transformation'
            assert isinstance(update_values[':t'], Decimal)

    @pytest.mark.mark_graphed
    def test_mark_object_as_graphed(self):
        with patch(patches.boto_patch, side_effect=intercept) as mock_boto:
            dynamo_driver = LeechDriver(table_name=blank_table_name)
            dynamo_driver.mark_object_as_graphed(identifier_stem=vertex_identifier_stem, id_value=vertex_id_value)
            assert mock_boto.called is True
            boto_args = mock_boto.call_args[0][0]
            boto_kwargs = mock_boto.call_args[0][1]
            assert boto_args == 'UpdateItem'
            assert boto_kwargs['Key'] == {'identifier_stem': str(vertex_identifier_stem),
                                          'sid_value': str(vertex_id_value)}
            update_expression = boto_kwargs['UpdateExpression']
            assert '#d=:d' in update_expression
            assert '#lts=:t' in update_expression
            assert '#lss=:s' in update_expression
            update_values = boto_kwargs['ExpressionAttributeValues']
            update_names = boto_kwargs['ExpressionAttributeNames']
            assert update_names['#d'] == 'disposition'
            assert update_names['#lss'] == 'last_stage_seen'
            assert update_names['#lts'] == 'last_time_seen'
            assert update_values[':d'] == 'processing'
            assert update_values[':s'] == 'graphing'
            assert isinstance(update_values[':t'], Decimal)

    @pytest.mark.mark_transform
    def test_set_transform_results(self):
        with patch(patches.boto_patch, side_effect=intercept) as mock_boto:
            dynamo_driver = LeechDriver(table_name=blank_table_name)
            potentials = [
                (first_potential_vertex, first_rule_entry)
            ]
            dynamo_driver.set_transform_results(
                change_potential_vertex, potentials,
                identifier_stem=change_potential_vertex.identifier_stem,
                id_value=change_potential_vertex.id_value)
            assert mock_boto.called is True
            boto_args = mock_boto.call_args[0][0]
            boto_kwargs = mock_boto.call_args[0][1]
            assert boto_args == 'UpdateItem'
            assert boto_kwargs['Key'] == {'identifier_stem': '#vertex#Change#{"id_source": "MBI", "id_type": "ChangeLogDetail", "id_name": "changelogdetail_id"}#',
                                          'sid_value': str(1230)}
            update_expression = boto_kwargs['UpdateExpression']
            assert '#ps=:ps' in update_expression
            assert '#o=:v' in update_expression
            assert '#p=:t' in update_expression
            assert '#d=:d' in update_expression
            assert '#lts=:t' in update_expression
            assert '#lss=:s' in update_expression
            update_values = boto_kwargs['ExpressionAttributeValues']
            update_names = boto_kwargs['ExpressionAttributeNames']
            test_vertex_properties = update_values[':v']
            test_internal_id = update_values[':i']
            test_disposition = update_values[':d']
            test_timestamp = update_values[':t']
            test_potentials = update_values[':ps']
            for test_potential in test_potentials:
                assert test_potential['assimilated'] is False
            assert 'detail_id' in test_vertex_properties
            assert 'id_source' in test_vertex_properties
            assert 'changelog_id' in test_vertex_properties
            assert 'data_dict_id' in test_vertex_properties
            assert 'detail_one' in test_vertex_properties
            assert 'detail_one_value' in test_vertex_properties
            assert 'detail_two' in test_vertex_properties
            assert isinstance(test_internal_id, str)
            assert isinstance(test_timestamp, Decimal)
            assert update_values[':s'] == 'transformation'
            assert test_disposition == 'graphing'

