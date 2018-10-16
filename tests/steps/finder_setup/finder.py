from mock import patch

from tests.steps.actor_setup import patches
from tests.steps.outside_setup.boto import intercept
from tests.steps.outside_setup.neptune import intercept_request
from toll_booth.alg_obj.graph.ogm.ogm import OgmReader


def run_finder(object_type, internal_id, context):
    with patch(patches.requests_get_patch, side_effect=intercept_request) as requests_mock, \
            patch(patches.boto_patch, side_effect=intercept) as mock_boto:
        ogm = OgmReader()
        context.mocks = {
            'requests': requests_mock,
            'boto': mock_boto
        }
        return ogm.find_object(internal_id, object_type == 'Edge')
