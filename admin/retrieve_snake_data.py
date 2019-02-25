import json

from toll_booth.alg_obj.serializers import AlgDecoder


def retrieve_snake_data(json_string):
    return json.loads(json_string, cls=AlgDecoder)


if __name__ == '__main__':
    target_json_string = '''
    	{"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_schema_entry!1551116614.785279.json"}}
    '''
    results = retrieve_snake_data(target_json_string)
    print(results)
