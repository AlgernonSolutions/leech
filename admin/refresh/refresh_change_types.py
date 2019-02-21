import json
import logging
import os

from jsonref import JsonRef
from jsonschema import validate

from admin.set_logging import set_logging
from toll_booth.alg_obj.aws.snakes.schema_snek import SchemaSnek


def refresh():
    logging.info('going to refresh the remote change type files')
    admin_file_name = os.path.dirname(__file__)
    admin_directory = os.path.dirname(admin_file_name)
    schema_file_path = os.path.join(admin_directory, 'starters', 'change_types.json')
    snek = SchemaSnek()
    snek.put_schema(schema_file_path, schema_name='change_types.json')
    logging.info('completed the update of the remote change type files')


def get_test():
    admin_file_name = os.path.dirname(__file__)
    admin_directory = os.path.dirname(admin_file_name)
    schema_file_path = os.path.join(admin_directory, 'starters', 'change_types.json')
    with open(schema_file_path) as test:
        test_schema = json.load(test)
        return test_schema


if __name__ == '__main__':
    set_logging()
    refresh()
