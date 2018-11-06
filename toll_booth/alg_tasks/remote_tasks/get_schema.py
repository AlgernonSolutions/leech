import json
import logging

from toll_booth.alg_obj.aws.sapper.schema_whisperer import SchemaWhisperer
from toll_booth.alg_obj.serializers import AlgEncoder
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def get_schema(*args, **kwargs):
    logging.info('firing the get schema command, with args/kwargs: %s/%s' % (args, kwargs))
    schema = SchemaWhisperer().get_schema()
    expected_return = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(schema, cls=AlgEncoder)
    }
    return expected_return
