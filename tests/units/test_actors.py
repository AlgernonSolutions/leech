import pytest
from mock import patch

from tests.steps.actor_setup import patches
from toll_booth.alg_obj.forge.dentist import Dentist
from toll_booth.alg_obj.forge.lizards import MonitorLizard
from toll_booth.alg_obj.forge.robot_in_disguise import DisguisedRobot
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex


@pytest.mark.actors
class TestActors:
    def test_lizard(self, identifier_stem):
        with patch(patches.x_ray_patch_begin), patch(patches.x_ray_patch_end), \
             patch(patches.send_patch), patch(patches.dynamo_driver_mark_working_patch, return_value=([1, 2, 3, 4], [1, 2, 3])):
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
        with patch(patches.send_patch) as mock_send, patch(patches.dynamo_driver_write_patch) as mock_write:
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

    def test_borg(self, assimilate_order):
        pass
