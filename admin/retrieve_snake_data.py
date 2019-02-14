import json

from toll_booth.alg_obj.serializers import AlgDecoder


def retrieve_snake_data(json_string):
    return json.loads(json_string, cls=AlgDecoder)


if __name__ == '__main__':
    target_json_string = '{"task_args": {"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1550093469.649069.json"}}, "pull_schema_entry": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_schema_entry!1550075460.658024.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1550075465.820633.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1550075469.952617.json"}}}}}, "config": {"_alg_class": "LeechConfig", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_workflow_configs": {"fungus": {"concurrency": 1, "time_outs": {"decision": "300"}}, "command_fungi": {"concurrency": 1, "labor_task_lists": [{"list_name": "credible", "number_threads": 1}], "time_outs": {"decision": "300"}}, "work_fip_links": {"concurrency": 1, "labor_task_lists": [{"list_name": "links", "number_threads": 5}], "time_outs": {"decision": "300"}}, "work_remote_id": {"concurrency": 1, "time_outs": {"decision": "300"}}, "work_remote_id_change_action": {"concurrency": 5, "time_outs": {"decision": "300"}}, "work_remote_id_change_type": {"concurrency": 1, "time_outs": {"decision": "300"}}, "fungal_leech": {"concurrency": 1, "time_outs": {"decision": "300"}}, "send_routine_reports": {"concurrency": 1, "time_outs": {"decision": "300"}, "labor_task_lists": [{"list_name": "credible_query", "number_threads": 1}]}}, "_task_configs": {"get_local_max_change_type_value": {"concurrency": 1}, "pull_change_types": {"concurrency": 1}, "unlink_old_id": {"concurrency": 250}, "unlink_old_ids": {"concurrency": 1, "task_list": "links"}, "link_new_id": {"concurrency": 250}, "link_new_ids": {"concurrency": 1, "task_list": "links"}, "put_new_id": {"concurrency": 250}, "put_new_ids": {"concurrency": 1, "task_list": "links"}, "graph_links": {"is_vpc": true, "concurrency": 1, "task_list": "links"}, "get_local_ids": {"concurrency": 1}, "get_remote_ids": {"concurrency": 1, "task_list": "credible"}, "work_remote_id_change_type": {"concurrency": 1, "task_list": "credible"}, "get_enrichment_for_change_action": {"concurrency": 1, "task_list": "credible"}, "pull_schema_entry": {"concurrency": 1}, "build_mapping": {"concurrency": 1}, "transform": {"concurrency": 1}, "assimilate": {"concurrency": 1}, "index": {"concurrency": 1}, "graph": {"concurrency": 1}, "generate_remote_id_change_data": {"concurrency": 1}, "get_report_args": {"concurrency": 1}, "query_data": {"concurrency": 10}, "query_credible_data": {"concurrency": 1, "task_list": "credible_query"}, "build_reports": {"concurrency": 10}, "send_reports": {"concurrency": 10}}}}, "versions": {"_alg_class": "Versions", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_workflow_versions": {"work_remote_id_change_type": 25, "work_remote_id_change_action": 25, "work_remote_id": 25, "work_fip_links": 13, "send_routine_reports": 17, "fungus": 25, "fungal_leech": 22, "command_fungi": 25}, "_task_versions": {"work_remote_id_change_type": 21, "unlink_old_ids": 16, "unlink_old_id": 10, "transform": 19, "send_reports": 12, "query_data": 14, "query_credible_data": 14, "put_new_ids": 16, "put_new_id": 10, "pull_schema_entry": 15, "pull_change_types": 21, "link_new_ids": 16, "link_new_id": 10, "index": 17, "graph_links": 4, "graph": 17, "get_report_args": 14, "get_remote_ids": 21, "get_local_max_change_type_value": 21, "get_local_ids": 21, "get_enrichment_for_change_action": 21, "generate_remote_id_change_data": 21, "build_reports": 14, "build_mapping": 21, "assimilate": 19}}}}'
    results = retrieve_snake_data(target_json_string)
    print(results)