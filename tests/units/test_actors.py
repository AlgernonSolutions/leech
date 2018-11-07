import pytest
from mock import patch

from tests.units.test_data import patches
from tests.units.test_data.data_setup.boto import intercept
from toll_booth.alg_obj.forge.borgs import SevenOfNine
from toll_booth.alg_obj.forge.dentist import Dentist
from toll_booth.alg_obj.forge.lizards import MonitorLizard
from toll_booth.alg_obj.forge.robot_in_disguise import DisguisedRobot
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex, PotentialEdge


@pytest.mark.actors
class TestActors:
    @pytest.mark.lizard
    def test_lizard(self, identifier_stem):
        with patch(patches.x_ray_patch_begin), patch(patches.x_ray_patch_end), \
             patch(patches.send_patch), patch(patches.dynamo_driver_mark_working_patch,
                                              return_value=([1, 2, 3, 4], [1, 2, 3])):
            lizard = MonitorLizard(identifier_stem=identifier_stem, sample_size=5)
            lizard.monitor()

    @pytest.mark.dentist
    def test_dentist(self, extraction_order):
        with patch(patches.small_add_patch) as mock_send, patch(patches.dynamo_driver_stage_patch) as mock_stage:
            dentist = Dentist(extraction_order)
            dentist.extract()
            assert mock_send.called is True
            assert mock_send.call_count == 1
            assert mock_stage.called is True
            assert mock_stage.call_count == 1
            mock_stage_call_args = mock_stage.call_args[0]
            assert mock_stage_call_args[0] == str(extraction_order.identifier_stem)
            assert mock_stage_call_args[1] == extraction_order.id_value
            assert mock_stage_call_args[2] == 'extraction'

    @pytest.mark.robot
    def test_disguised_robot(self, transform_order):
        with patch(patches.send_patch) as mock_send, patch(patches.dynamo_driver_write_patch) as mock_write, patch(patches.boto_patch, side_effect=intercept):
            disguised_robot = DisguisedRobot(transform_order)
            disguised_robot.transform()
            assert mock_send.called is True
            assert mock_send.call_count == 1
            assert mock_write.called is True
            assert mock_write.call_count == 1
            mock_write_args = mock_write.call_args[0]
            written_vertex = mock_write_args[0]
            assert isinstance(written_vertex, PotentialVertex)
            assert mock_write_args[1] == 'transformation'

    def test_borg_assimilate_identifiable_potential(self, identifiable_assimilate_order):
        with patch(patches.dynamo_driver_write_patch) as vertex_write, \
                patch(patches.dynamo_driver_write_edge_patch) as edge_write, \
                patch(patches.dynamo_driver_mark_stub_patch) as stub_write:
            borg = SevenOfNine(identifiable_assimilate_order)
            borg.assimilate()
            assert stub_write.called is False

            vertex_write_commands = vertex_write.call_args[0]
            assert vertex_write.called is True
            assert vertex_write_commands[0] == identifiable_assimilate_order.potential_vertex
            assert vertex_write_commands[1] == 'assimilation'

            edge_write_commands = edge_write.call_args[0]
            assert edge_write.called is True
            assert isinstance(edge_write_commands[0], PotentialEdge)
            assert edge_write_commands[1] == 'assimilation'

    def test_borg_assimilate_stubbed_potential(self, stubbed_assimilate_order):
        with patch(patches.dynamo_driver_write_patch) as vertex_write, \
                patch(patches.dynamo_driver_write_edge_patch) as edge_write, \
                patch(patches.dynamo_driver_mark_stub_patch) as stub_write:
            borg = SevenOfNine(stubbed_assimilate_order)
            borg.assimilate()
            assert vertex_write.called is False

            stub_write_commands = stub_write.call_args[0]
            source_vertex = stubbed_assimilate_order.source_vertex
            potential_vertex = stubbed_assimilate_order.potential_vertex
            rule_entry = stubbed_assimilate_order.rule_entry
            assert stub_write.called is True
            assert stub_write_commands[0] == potential_vertex.object_type
            assert isinstance(stub_write_commands[1], dict)
            assert stub_write_commands[2] == source_vertex.internal_id
            assert stub_write_commands[3] == rule_entry.edge_type
