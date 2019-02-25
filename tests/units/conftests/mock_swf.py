import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from tests.units.conftests.bad_subtask_events import bad_subtask_events
from tests.units.test_data import patches
from tests.units.test_data.data_setup.boto import intercept
from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.serializers import AlgDecoder


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
    (
        'batch_generate_remote_id_change_data',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1551137994.266595.json"}}, "pull_schema_entry": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_schema_entry!1551119330.035637.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1551119333.158926.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1551119334.148466.json"}}, "work_fip_links": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_fip_links!1551137995.446181.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1551119387.394579.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1551119395.640103.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id12290!1551137995.98097.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type9!1551120002.64226.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1551119420.189705.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action108!1551120008.486399.json"}}, "get_enrichment_for_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_enrichment_for_change_action!1551120049.601034.json"}}, "batch_generate_remote_id_change_data": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/batch_generate_remote_id_change_data0-batch_generate_remote_id_change_data-work_action-ASSIGN CLIENT CASE MANAGER-work_type-Other-work_id-12290-99!1551120055.274447.json"}}}}}'
    )
]

mule_team_params = (
    'get_enrichment_for_change_action',
    '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548208189.812885.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548208190.72171.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548190253.498273.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548190253.499673.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548190253.499693.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4851!1548190253.934165.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548190306.370741.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548190281.620162.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action325!1548190343.01072.json"}}}}}'
)

work_change_type_params = (
    'work_remote_id_change_type',
    '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548276521.454877.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548276522.666301.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548258531.124235.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548258531.125017.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548258531.12503.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id5177!1548258531.6202.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type10!1548258539.876893.json"}}}}}'
)

generate_remote_change_data_params = (
    'generate_remote_id_change_data',
    '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks","value": {"_arguments": { "fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548365624.024219.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548365627.193235.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes","value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData","_alg_module": "toll_booth.alg_obj.aws.snakes.snakes","value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes","value": {"pointer": "the-leech#cache/put_new_ids!1548347632.313116.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548347632.313808.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548347632.313821.json"}},"pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4892!1548347632.792488.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548347762.058381.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548347743.420804.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action325!1548347827.305373.json"}}, "get_enrichment_for_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_enrichment_for_change_action!1548347852.198812.json"}}, "generate_remote_id_change_data": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/generate_remote_id_change_data{\"Staff Name\": \"Michelle Carr\", \"Date\": datetime.datetime(2018, 3, 16, 10, 31, 19), \"Category\": \"Print\", \"Action\": \"PRINT CLIENT MEDICATIONS\", \"Entity\": \"LEWIS, E\", \"Consumer ID\": \"12428\", \"Service ID\": None, \"Done By\": \"Carr, M\", \"Record\": \"12428(Clients)\", \"Description\": None, \"UTCDate\": datetime.datetime(2018, 3, 16, 15, 31, 19, tzinfo=<UTC>)}!1548347857.249772.json"}}}}}'
)


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


@pytest.fixture
def mule_team_arg():
    task_args = json.loads(mule_team_params[1], cls=AlgDecoder)
    return {'task_name': 'get_enrichment_for_change_action', 'task_args': task_args}


@pytest.fixture
def work_change_type_arg():
    task_args = json.loads(work_change_type_params[1], cls=AlgDecoder)
    return {'task_name': 'work_remote_id_change_type', 'task_args': task_args}


@pytest.fixture
def generate_remote_id_change_data_arg():
    task_args = json.loads(generate_remote_change_data_params[1], cls=AlgDecoder)
    return {'task_name': 'generate_remote_id_change_data', 'task_args': task_args}


@pytest.fixture
def mock_activity_poll():
    poll_patch = patches.get_poll_patch()
    mock_poll = poll_patch.start()
    yield mock_poll
    poll_patch.stop()


@pytest.fixture
def mock_decision_poll():
    poll_patch = patches.get_poll_patch(True)
    mock_poll = poll_patch.start()
    yield mock_poll
    poll_patch.stop()


@pytest.fixture
def spiked_decision_poll():
    poll_patch = patches.get_poll_patch(True)
    mock_poll = poll_patch.start()
    from toll_booth.alg_obj.aws.gentlemen.events.history import WorkflowHistory
    poll_response = {
        'events': bad_subtask_events,
        'workflowType': {'name': 'test'},
        'taskToken': '123NotaTaskToken',
        'workflowExecution': {
            'workflowId': '123321',
            'runId': '98766789'
        }
    }
    work_history = WorkflowHistory.parse_from_poll('TheLeech', poll_response)
    yield mock_poll
    poll_patch.stop()


@pytest.fixture
def mock_work_history():
    from tests.units.conftests.fungal_leech_events import fungal_leech_events
    from toll_booth.alg_obj.aws.gentlemen.events.history import WorkflowHistory
    history = WorkflowHistory.parse_from_poll('TheLeech', fungal_leech_events)
    return history
