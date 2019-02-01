import json
import logging
import os

from jsonref import JsonRef
from jsonschema import validate

from admin.set_logging import set_logging
from toll_booth.alg_obj.posts.fedoras.report_schema import ReportSchema


def refresh():
    logging.info('going to refresh the remote report schema files')
    admin_file_name = os.path.dirname(__file__)
    admin_directory = os.path.dirname(admin_file_name)
    schema_file_path = os.path.join(admin_directory, 'starters', 'report_schemas', 'reports.json')
    master_file_path = os.path.join(admin_directory, 'starters', 'validation_schemas', 'report_validation_schema.json')
    ReportSchema.post(schema_file_path, master_file_path)
    logging.info('completed the update of the remote report schema files')


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
    set_logging()
    refresh()
