from mock import patch

from tests.steps.actor_setup import patches
from tests.steps.outside_setup.boto import intercept
from toll_booth.alg_obj.forge.borgs import SevenOfNine


class MockAssimilator:
    @classmethod
    def run(cls, context):
        assimilate_order = context.assimilate_order
        with patch(patches.send_patch, return_value=1) as mock_send, \
                patch(patches.add_patch, return_value=1) as mock_add, \
                patch(patches.requests_patch) as mock_request, \
                patch(patches.boto_patch, side_effect=intercept) as mock_boto, \
                patch(patches.x_ray_patch_begin), patch(patches.x_ray_patch_end):
            context.assimilate_mocks = {
                'send': mock_send,
                'add': mock_add,
                'boto': mock_boto,
                'request': mock_request
            }
            seven = SevenOfNine(assimilate_order)
            seven.assimilate()
