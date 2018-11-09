import pytest

from toll_booth.alg_obj.forge.robot_in_disguise import DisguisedRobot


@pytest.mark.robot
class TestDisguisedRobot:
    def test_transformation(self, transform_order, robot_test_environment):
        robot = DisguisedRobot(transform_order)
        results = robot.transform()
        print()
