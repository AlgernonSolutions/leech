import json
import logging

from toll_booth.alg_obj.aws.aws_obj.sapper import SchemaWhisperer
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def update_schema(*args, **kwargs):
    logging.info('firing the update schema command, with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    working_schema = json.loads(task_args['schema'])
    with SchemaWhisperer() as writer:
        for vertex_entry in working_schema['vertex']:
            writer.write_schema_entry(vertex_entry)
        for edge_entry in working_schema['edge']:
            writer.write_schema_entry(edge_entry)
    expected_return = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps({'success': True})
    }
    return expected_return
