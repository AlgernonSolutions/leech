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

