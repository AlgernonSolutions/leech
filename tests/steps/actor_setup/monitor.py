from mock import patch

from tests.steps.outside_setup.boto import MockBoto
from tests.steps.outside_setup.credible_ws import MockCredibleWsGenerator as WsGen
from tests.steps.schema_setup.object_identifier import MockObjectIdentifier
from tests.steps.outside_setup.redis import MockRedis as MRedis
from toll_booth.alg_obj.forge.lizards import MonitorLizard
from tests.steps.actor_setup import patches


class MockMonitor:
    @classmethod
    def run(cls, context):
        with patch(patches.redis_pipeline_patch, return_value=MRedis.get_for_monitor(context)) as mock_redis_pipeline, \
                patch(patches.redis_client_patch, return_value=False) as mock_redis_client, \
                patch(patches.boto_patch, side_effect=MockBoto.get_for_monitor(context)) as mock_boto, \
                patch(patches.send_patch, return_value=1) as mock_send, \
                patch(patches.requests_patch, return_value=WsGen.get_for_max_min(context)) as mock_request, \
                patch(patches.add_patch, return_value=1) as mock_add, \
                patch(patches.x_ray_patch_begin), patch(patches.x_ray_patch_end):
            context.monitor_mocks = {
                'redis_pipeline': mock_redis_pipeline,
                'redis_client': mock_redis_client,
                'boto': mock_boto,
                'send': mock_send,
                'add': mock_add,
                'request': mock_request
            }
            object_identifier = MockObjectIdentifier.get(context)
            lizard = MonitorLizard(
                object_identifier=object_identifier,
                index_name='test_index_name',
                extraction_source_name=MockObjectIdentifier.get_extraction_source(context)
            )
            lizard.monitor()
