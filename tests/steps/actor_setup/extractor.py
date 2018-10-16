from mock import patch

from tests.steps.actor_setup import patches
from tests.steps.outside_setup.credible_ws import MockCredibleWsGenerator as WsGen
from toll_booth.alg_obj.forge.dentist import Dentist


class MockExtractor:
    @classmethod
    def run(cls, context):
        extraction_order = context.extraction_order
        with patch(patches.send_patch, return_value=1) as mock_send, \
                patch(patches.requests_patch, side_effect=WsGen.get_for_extractor(context)) as mock_request, \
                patch(patches.add_patch, return_value=1) as mock_add, \
                patch(patches.x_ray_patch_begin), patch(patches.x_ray_patch_end):
            context.extractor_mocks = {
                'send': mock_send,
                'add': mock_add,
                'request': mock_request
            }
            dentist = Dentist(extraction_order)
            dentist.extract()
