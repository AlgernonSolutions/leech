import json

from toll_booth.alg_obj.serializers import AlgDecoder


def retrieve_snake_data(json_string):
    return json.loads(json_string, cls=AlgDecoder)


if __name__ == '__main__':
    target_json_string = '''
        	{"task_name": "rough_housing", "task_args": {"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"ruffianing": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/ruffianing!1554133417.307537.json"}}, "rough_housing": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/rough_housingci_ICFS_2019-04-01_v4_TheLeech$send_canned_reports$credible$True!1554133417.40729.json"}}}}}, "flow_id": "ruffianing", "run_id": "2270laW6NgihpIdLvhlz5673Hedbq+n2MR7ed4ghFvPMA=", "task_id": "ci_ICFS_2019-04-01_v4_TheLeech$send_canned_reports$credible$True", "register_results": true}
    '''
    results = retrieve_snake_data(target_json_string)
    print(results)
