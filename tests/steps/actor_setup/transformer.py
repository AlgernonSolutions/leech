from mock import patch

from tests.steps.actor_setup import patches
from tests.steps.outside_setup.boto import intercept
from toll_booth.alg_obj.forge.robot_in_disguise import DisguisedRobot


class MockTransformer:
    @classmethod
    def run(cls, context):
        transform_order = context.transform_order
        with patch(patches.send_patch, return_value=1) as mock_send, \
                patch(patches.add_patch, return_value=1) as mock_add, \
                patch(patches.boto_patch, side_effect=intercept) as mock_boto, \
                patch(patches.x_ray_patch_begin), patch(patches.x_ray_patch_end):
            context.transformer_mocks = {
                'send': mock_send,
                'add': mock_add,
                'boto': mock_boto
            }
            transformer = DisguisedRobot(transform_order)
            transformer.transform()
