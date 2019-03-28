import json

from toll_booth.alg_obj.serializers import AlgDecoder


def retrieve_snake_data(json_string):
    return json.loads(json_string, cls=AlgDecoder)


if __name__ == '__main__':
    target_json_string = '''
    {"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"ruffianing": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/ruffianing!1553804694.013505.json"}}, "rough_housing": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/rough_housingwork_id-12295-501_TheLeech$work_remote_id$work_id-12295-501$False!1553804694.11317.json"}}}}}
    '''
    results = retrieve_snake_data(target_json_string)
    print(results)
