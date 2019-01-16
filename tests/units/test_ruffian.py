import json

import pytest

from toll_booth.alg_obj.aws.snakes.snakes import StoredData
from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder
from toll_booth.alg_tasks.ruffians import labor, lambda_labor


@pytest.mark.usefixtures('silence_x_ray')
class TestRuffian:
    @pytest.mark.ruffian_labor
    def test_ruffian_labor(self, mock_context):
        event = {
          "work_list": {
            "list_name": "credible",
            "number_threads": 1,
            "is_vpc": False
          },
          "domain_name": "TheLeech"
        }
        labor(event, mock_context)

    @pytest.mark.ruffian_lambda_labor
    def test_lambda_labor(self, lambda_labor_arg, mock_context):
        results = lambda_labor(lambda_labor_arg, mock_context)
        loaded_results = json.loads(results, cls=AlgDecoder)
        assert isinstance(results, str)
        assert isinstance(loaded_results, StoredData)
