import json
import logging
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.gentlemen.tasks import Versions, LeechConfig
from toll_booth.alg_obj.aws.overseer.overseer import Overseer
from toll_booth.alg_obj.aws.ruffians.ruffian import RuffianRoost
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder
from toll_booth.alg_tasks.lambda_logging import lambda_logged


@lambda_logged
def start_flow(*args):
    event = args[0]
    logging.info(f'received a call to fire a starter task, event: {event}')
    domain_name = event['domain_name']
    versions = event.get('versions', Versions.retrieve(domain_name))
    config = event.get('config', LeechConfig.retrieve())
    provided_flow_id = event['flow_id']
    flow_id = _generate_flow_id(provided_flow_id, **event)
    flow_name = event['flow_name']
    input_string = event.get('input_string', '{}')
    input_value = json.loads(input_string, cls=AlgDecoder)
    if input_value.get('config', None) is None:
        input_value['config'] = config
    if input_value.get('versions', None) is None:
        input_value['versions'] = versions
    input_string = json.dumps(input_value, cls=AlgEncoder)
    run_config = event.get('run_config', {})
    overseer = Overseer.start(domain_name, versions)
    client = boto3.client('swf')
    decider_ruffian = RuffianRoost.generate_decider_ruffian(domain_name, flow_id, flow_name, run_config)
    start_args = {
        'domain': domain_name,
        'workflowId': flow_id,
        'workflowType': {
            'name': flow_name,
            'version': str(versions.workflow_versions[flow_name])
        },
        'taskList': {'name': flow_id},
        'childPolicy': 'TERMINATE',
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
    if input_string:
        start_args['input'] = input_string
    start_results = {}
    try:
        start_results['workflow'] = client.start_workflow_execution(**start_args)
    except ClientError as e:
        if e.response['Error']['Code'] != 'WorkflowExecutionAlreadyStartedFault':
            raise e
        start_results['workflow'] = 'workflow already running'
    start_results['overseer'] = overseer.signal('start_ruffian', flow_id, config, [decider_ruffian])
    return start_results


def _generate_flow_id(provided_flow_id, **kwargs):
    if provided_flow_id == 'daily_reports':
        input_string = kwargs['input_string']
        input_value = json.loads(input_string, cls=AlgDecoder)
        iso_date_format = '%Y-%m-%d'
        now = datetime.now()
        flow_id = f'ci_{input_value["id_source"]}_{now.strftime(iso_date_format)}'
        return flow_id
    return provided_flow_id
