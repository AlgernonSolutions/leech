import json

import pytest

from toll_booth.alg_obj.aws.snakes.snakes import StoredData
from toll_booth.alg_obj.serializers import AlgDecoder
from toll_booth.alg_tasks.ruffians import lambda_labor


class TestTasks:
    @pytest.mark.get_enrichment
    def test_get_enrichment_data(self, mock_context):
        input_data = '{"task_name": "get_enrichment_for_change_action", "task_args": {"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1547848388.645157.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1547848391.063022.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1547829791.785334.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1547829777.66411.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1547830397.354854.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1547830397.356043.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1547830397.356062.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1547829805.946796.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1547829812.372569.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4835!1547830397.816213.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1547830508.253227.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1547830486.113847.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action325!1547830568.981665.json"}}}}}, "register_results": true}'
        input_args = json.loads(input_data)
        results = lambda_labor(input_args, mock_context)
        assert isinstance(results, str)
        unpacked_results = json.loads(results, cls=AlgDecoder)
        assert isinstance(unpacked_results, StoredData)
