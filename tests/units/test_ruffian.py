import json

import pytest

from toll_booth.alg_obj.aws.snakes.snakes import StoredData
from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder
from toll_booth.alg_tasks.rivers.flows.leech.fungal_leech import fungal_leech
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
    # @pytest.mark.usefixtures('mock_activity_poll')
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
    # @pytest.mark.usefixtures('mock_decision_poll')
    def test_ruffian_decider_graceful_exit(self, timed_mock_context):
        event = {
            "work_list": "credible",
            "domain_name": "TheLeech",
            "warn_seconds": 275
        }
        decide(event, timed_mock_context)

    @pytest.mark.ruffian_lambda_labor
    def test_lambda_labor(self, lambda_labor_arg, mock_context):
        lambda_labor_arg['flow_id'] = '123some_flow_id'
        lambda_labor_arg['run_id'] = '123_some_run_id'
        lambda_labor_arg['task_id'] = '123_some_task_id'
        sent_args = json.loads(json.dumps(lambda_labor_arg, cls=AlgEncoder))
        results = lambda_labor(sent_args, mock_context)
        loaded_results = json.loads(results, cls=AlgDecoder)
        assert isinstance(results, str)
        assert isinstance(loaded_results, StoredData)

    @pytest.mark.tasks
    def test_tasks(self, custom_task_args, mock_context):
        task_data = {'id_source': 'ICFS'}
        task_args = custom_task_args(task_data)
        lambda_args = {
            'task_name': 'get_productivity_report_data',
            'task_args': task_args,
            'flow_id': '123some_flow_id',
            'run_id': '123_some_run_id',
            'task_id': '123_some_task_id'
        }
        sent_args = json.loads(json.dumps(lambda_args, cls=AlgEncoder))
        results = lambda_labor(sent_args, mock_context)
        loaded_results = json.loads(results, cls=AlgDecoder)
        assert isinstance(results, str)
        assert isinstance(loaded_results, StoredData)

    @pytest.mark.ruffian_mule_team
    def test_ruffian_mule_team(self, mule_team_arg, mock_context):
        sent_args = json.loads(json.dumps(mule_team_arg, cls=AlgEncoder))
        results = lambda_labor(sent_args, mock_context)
        loaded_results = json.loads(results, cls=AlgDecoder)
        assert isinstance(results, str)
        assert isinstance(loaded_results, StoredData)

    @pytest.mark.ruffian_work_change_type
    def test_ruffian_work_change_type(self, work_change_type_arg, mock_context):
        sent_args = json.loads(json.dumps(work_change_type_arg, cls=AlgEncoder))
        results = lambda_labor(sent_args, mock_context)
        loaded_results = json.loads(results, cls=AlgDecoder)
        assert isinstance(results, str)
        assert isinstance(loaded_results, StoredData)

    @pytest.mark.ruffian_work_bad_subtask_events
    def test_ruffian_bad_subtask_events(self, mock_work_history, mock_context):
        fungal_leech(mock_work_history, context=mock_context)
        print(mock_work_history)

    @pytest.mark.ruffian_generate_remote_id_change_data
    def test_generate_remote_id_change_data(self, generate_remote_id_change_data_arg, mock_context):
        sent_args = json.loads(json.dumps(generate_remote_id_change_data_arg, cls=AlgEncoder))
        results = lambda_labor(sent_args, mock_context)
        loaded_results = json.loads(results, cls=AlgDecoder)
        assert isinstance(results, str)
        assert isinstance(loaded_results, StoredData)

    @pytest.mark.test_hanging_decider
    def test_hanging_decider(self, hanging_decider_arg, mock_context):
        sent_args = json.loads(json.dumps(hanging_decider_arg, cls=AlgEncoder))
        decide(sent_args, mock_context)
