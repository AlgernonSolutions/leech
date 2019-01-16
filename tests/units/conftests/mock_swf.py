import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from tests.units.test_data import patches
from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder


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
    # ('get_remote_ids', '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1547632256.888168.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1547632257.494224.json"}}}}}'),
    # ('get_local_ids', '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1547632256.888168.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1547632257.494224.json"}}}}}'),
    ('put_new_ids', '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1547632610.605488.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1547632611.220036.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1547614647.89166.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1547614625.421932.json"}}}}}'),
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
    task_args = json.loads(params[1], cls=AlgDecoder)
    return {'task_name': params[0], 'task_args': task_args}
