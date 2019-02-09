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
        'unlink_old_ids',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"work_fip_links": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/scratch_35!1549688184.288699.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_idsunlink_old_id-39!1549688187.419778.json"}}}}}'
    ),
    (
        'put_new_ids',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"work_fip_links": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/scratch_35!1549685482.021982.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_idsput_new_id-39!1549685517.075274.json"}}}}}'
    ),
    (
        'put_new_ids',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"work_fip_links": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/scratch_35!1549685482.021982.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_idsput_new_id-39!1549685517.075274.json"}}}}}'
    ),
    (
        "get_local_ids",
        ' {"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1549671680.549495.json"}}, "pull_schema_entry": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_schema_entry!1549653347.748152.json"}}}}}'
    ),
    (
        'pull_schema_entry',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1549659941.578722.json"}}}}}'
    ),

    (
        "get_remote_ids",
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1549654845.07492.json"}}}}}'
    ),
    (
        'build_reports',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"send_routine_reports": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/send_routine_reports!1549421979.531483.json"}}, "get_report_args": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_report_args!1549403925.314252.json"}}, "query_credible_data": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/query_credible_data!1549403932.282184.json"}}}}}'
    ),
    (
        'get_report_args',
        ' {"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"send_routine_reports": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/send_routine_reports!1549397502.225521.json"}}}}}'
    ),
    (
        'query_credible_data',
        ' {"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"send_routine_reports": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/send_routine_reports!1549331887.985011.json"}}, "get_report_args": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_report_args!1549313834.902911.json"}}, "query_credible_data": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/query_credible_dataencounters-11!1549331888.441699.json"}}}}}'
    ),
    (
        'assimilate',
        ' {"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548882940.150575.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548882940.606091.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548864759.474746.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548864738.575598.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548864945.621849.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548864945.622736.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548864945.622749.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548864774.843153.json"}}, "pull_schema_entry": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_schema_entry!1548864783.00537.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548864791.924044.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4901!1548864946.533986.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548865080.091061.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548865062.976749.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action116!1548865263.126111.json"}}, "get_enrichment_for_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_enrichment_for_change_action!1548865298.312812.json"}}, "generate_remote_id_change_data": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/generate_remote_id_change_data!1548865310.333731.json"}}, "fungal_leech": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungal_leech!1548865318.007308.json"}}, "transform": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/transform!1548865328.453922.json"}}, "assimilate": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/assimilateassimilate-[_by_emp_id_]-[ExternalId-4901]-fungal_leech-work_action-ASSIGN CREDENTIALS-work_type-Other-work_id-4901-f-43!1548865333.338664.json"}}}}}'
    ),
    (
        'transform',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548814146.567113.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548814147.522429.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548796181.497873.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548796160.366373.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548796222.66278.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548796222.680091.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548796222.680111.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548796198.701642.json"}}, "pull_schema_entry": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_schema_entry!1548796207.034714.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548796216.25804.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4439!1548796223.256025.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548796381.682067.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548796368.096558.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action108!1548796496.788076.json"}}, "get_enrichment_for_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_enrichment_for_change_action!1548796533.221404.json"}}, "generate_remote_id_change_data": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/generate_remote_id_change_data!1548796588.790934.json"}}, "fungal_leech": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungal_leech!1548796691.867588.json"}}}}}'
    ),
    (
        'pull_schema_entry',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548811007.193772.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548811007.683145.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548793034.337477.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548793021.459525.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548793069.033307.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548793069.034104.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548793069.034118.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548793049.134773.json"}}}}}'
    ),

    (
        'transform',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548803751.12553.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548803751.295948.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548785778.928072.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548785758.724864.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548785809.957393.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548785809.958167.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548785809.95818.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548785795.56766.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548785803.910505.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id5354!1548785810.393987.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548785896.405131.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548785880.283882.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action321!1548785944.401045.json"}}, "get_enrichment_for_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_enrichment_for_change_action!1548785994.066107.json"}}, "generate_remote_id_change_data": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/generate_remote_id_change_data!1548786043.797206.json"}}, "fungal_leech": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungal_leech!1548786093.036721.json"}}}}}'
    ),
    (
        'generate_remote_id_change_data',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548716266.899264.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548716267.756178.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548682662.11541.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548682643.312084.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548698272.314553.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548698272.315771.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548698272.31579.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548682675.772451.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548682682.198919.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4912!1548698272.857392.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548698315.425891.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548698301.608508.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action36!1548698323.219898.json"}}, "get_enrichment_for_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_enrichment_for_change_action!1548698448.309248.json"}}, "generate_remote_id_change_data": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/generate_remote_id_change_data8204429961177749533!1548698452.590164.json"}}}}}'
    ),
    (
        'get_remote_ids',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1547632256.888168.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1547632257.494224.json"}}}}}'),
    (
        'get_local_ids',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1547632256.888168.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1547632257.494224.json"}}}}}'),
    (
        'put_new_ids',
        '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1547632610.605488.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1547632611.220036.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1547614647.89166.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1547614625.421932.json"}}}}}'
    ),
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
