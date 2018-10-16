from mock import patch

from tests.steps.actor_setup import patches
from tests.steps.outside_setup.boto import intercept
from tests.steps.outside_setup.neptune import MockNeptune
from toll_booth.alg_obj.forge.forklifts import ReachTruck


class MockLoader:
    @classmethod
    def run(cls, context):
        load_order = context.load_order
        with patch(patches.send_patch, return_value=1) as mock_send, \
                patch(patches.add_patch, return_value=1) as mock_add, \
                patch(patches.redis_client_patch, return_value=False) as mock_redis_client, \
                patch(patches.redis_pipeline_patch), \
                patch(patches.redis_load_patch) as mock_redis_pipeline, \
                patch(patches.boto_patch, side_effect=intercept), \
                patch(patches.requests_get_patch, return_value=MockNeptune.get_for_loader(context)) as mock_request, \
                patch(patches.x_ray_patch_begin), patch(patches.x_ray_patch_end):
            reach_truck = ReachTruck(load_order)
            reach_truck.load()
            context.assimilate_mocks = {
                'send': mock_send,
                'add': mock_add,
                'redis_client': mock_redis_client,
                'redis_pipeline': mock_redis_pipeline,
                'request': mock_request
            }
