import json
import logging
import os

from jsonref import JsonRef
from jsonschema import validate

from admin.set_logging import set_logging
from toll_booth.alg_obj.aws.gentlemen.tasks import LeechConfig
from toll_booth.alg_obj.aws.snakes.schema_snek import SchemaSnek


def refresh():
    logging.info('going to refresh the remote config files')
    admin_file_name = os.path.dirname(__file__)
    top_level = os.path.dirname(admin_file_name)
    config_path = ('tests', 'units', 'test_data', 'configs', 'config.json')
    master_schema_path = ('tests', 'units', 'test_data', 'configs', 'master_config.json')
    config_file_path = os.path.join(top_level, *config_path)
    validation_file_path = os.path.join(top_level, *master_schema_path)
    LeechConfig.post(config_file_path, validation_file_path)
    logging.info('completed the update of the remote config files')


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
