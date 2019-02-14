import json

from toll_booth.alg_obj.serializers import AlgDecoder


def retrieve_snake_data(json_string):
    return json.loads(json_string, cls=AlgDecoder)


if __name__ == '__main__':
    target_json_string = '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1550191808.2372.json"}}, "pull_schema_entry": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_schema_entry!1550173296.692911.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1550173807.053414.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1550173303.975128.json"}}}}}'
    results = retrieve_snake_data(target_json_string)
    print(results)
