import json
import os

import boto3
from jsonref import JsonRef
from jsonschema import validate

from toll_booth.alg_obj.graph.schemata.schema import Schema


def refresh():
    test_file_name = os.path.dirname(__file__)

    s3 = boto3.resource('s3')
    s3.Bucket('algernonsolutions-test').upload_file(os.path.join(test_file_name, 'schema.json'),
                                                    'config/schema.json')
    s3.Bucket('algernonsolutions-test').upload_file(os.path.join(test_file_name, 'master_schema.json'),
                                                    'config/master_schema.json')
    Schema.post('algernonsolutions-test')


def get_test():
    test_file_name = os.path.dirname(__file__)
    test_schema_file_path = os.path.join(test_file_name, 'schema.json')
    master_schema_file_path = os.path.join(test_file_name, 'master_schema.json')
    with open(test_schema_file_path) as test, open(master_schema_file_path) as master:
        test_schema = json.load(test)
        test_schema = JsonRef.replace_refs(test_schema)
        master_schema = json.load(master)
        validate(test_schema, master_schema)
        return test_schema


if __name__ == '__main__':
    refresh()
