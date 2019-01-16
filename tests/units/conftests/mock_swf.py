from datetime import datetime
from unittest.mock import MagicMock

import pytest

from tests.units.test_data import patches
from toll_booth.alg_obj.aws.gentlemen.events.events import Event


class MockSwfEvent:
    @classmethod
    def for_workflow_started(cls, input_string, task_list_name, flow_type, **kwargs):
        attributes = {
            'workflowExecutionStartedEventAttributes': {
                'input': input_string,
                'taskList': {'name': task_list_name},
                'workflowType': {
                    'name': flow_type,
                    'version': kwargs.get('version', '1')
                }
            }
        }
        return Event(1, 'WorkflowExecutionStarted', datetime.utcnow(), attributes)


_lambda_labor_params = [
    ('get_remote_ids', """{"_alg_class": "<class 'toll_booth.alg_obj.aws.gentlemen.tasks.TaskArguments'>", "value": {"_arguments": {"fungus": {"_alg_class": "<class 'toll_booth.alg_obj.aws.snakes.snakes.StoredData'>", "value": {"pointer": "the-leech#cache/fungus!1547588514.708972.json"}}, "command_fungi": {"_alg_class": "<class 'toll_booth.alg_obj.aws.snakes.snakes.StoredData'>", "value": {"pointer": "the-leech#cache/command_fungi!1547588517.265948.json"}}}}}"""),
    ('get_local_ids', """{"_alg_class": "<class 'toll_booth.alg_obj.aws.gentlemen.tasks.TaskArguments'>", "value": {"_arguments": {"fungus": {"_alg_class": "<class 'toll_booth.alg_obj.aws.snakes.snakes.StoredData'>", "value": {"pointer": "the-leech#cache/fungus!1547588514.708972.json"}}, "command_fungi": {"_alg_class": "<class 'toll_booth.alg_obj.aws.snakes.snakes.StoredData'>", "value": {"pointer": "the-leech#cache/command_fungi!1547588517.265948.json"}}}}}"""),
    ('put_new_ids', """{"_alg_class": "<class 'toll_booth.alg_obj.aws.gentlemen.tasks.TaskArguments'>", "value": {"_arguments": {"fungus": {"_alg_class": "<class 'toll_booth.alg_obj.aws.snakes.snakes.StoredData'>", "value": {"pointer": "the-leech#cache/fungus!1547588514.708972.json"}}, "command_fungi": {"_alg_class": "<class 'toll_booth.alg_obj.aws.snakes.snakes.StoredData'>", "value": {"pointer": "the-leech#cache/command_fungi!1547588517.265948.json"}}}}}"""),
]

@pytest.fixture
def initial_decision_history():
    return MagicMock()


@pytest.fixture
def mock_versions():
    version_patch = patches.get_version_patch()
    mock_version = version_patch.start()
    yield mock_version
    version_patch.stop()


@pytest.fixture
def mock_config():
    config_patch = patches.get_config_patch()
    mock_config = config_patch.start()
    mock_config.concurrency.return_value = 1
    yield mock_config
    config_patch.stop()


@pytest.fixture(params=_lambda_labor_params)
def lambda_labor_arg(request):
    params = request.param
    return {'task_name': params[0], 'task_args': params[1]}
