import json

import pytest

from toll_booth.alg_obj.aws.snakes.snakes import StoredData
from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder
from toll_booth.alg_tasks.ruffians import labor, lambda_labor, decide


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
            "domain_name": "TheLeech",
            "warn_seconds": 275
        }
        labor(event, mock_context)

    @pytest.mark.ruffian_exit
    @pytest.mark.usefixtures('mock_activity_poll')
    def test_ruffian_labor_graceful_exit(self, timed_mock_context):
        event = {
            "work_list": {
                "list_name": "credible",
                "number_threads": 1,
                "is_vpc": False
            },
            "domain_name": "TheLeech",
            "warn_seconds": 275
        }
        labor(event, timed_mock_context)

    @pytest.mark.ruffian_exit
    @pytest.mark.usefixtures('mock_decision_poll')
    def test_ruffian_decider_graceful_exit(self, timed_mock_context):
        event = {
            "work_list": "credible",
            "domain_name": "TheLeech",
            "warn_seconds": 275
        }
        decide(event, timed_mock_context)

    @pytest.mark.ruffian_lambda_labor
    def test_lambda_labor(self, lambda_labor_arg, mock_context):
        sent_args = json.loads(json.dumps(lambda_labor_arg, cls=AlgEncoder))
        results = lambda_labor(sent_args, mock_context)
        loaded_results = json.loads(results, cls=AlgDecoder)
        assert isinstance(results, str)
        assert isinstance(loaded_results, StoredData)
