import boto3

from toll_booth.alg_obj.aws.gentlemen.tasks import Versions


def start_flow(event, context):
    flow_id = event['flow_id']
    domain_name = event['domain_name']
    flow_name = event['flow_name']
    input_string = event.get('input_string')
    client = boto3.client('swf')
    versions = Versions.retrieve(domain_name)
    start_args = {
        'domain': domain_name,
        'workflowId': flow_id,
        'workflowType': {
            'name': flow_name,
            'version': str(versions.workflow_versions[flow_name])
        },
        'taskList': {'name': flow_name},
        'childPolicy': 'TERMINATE',
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
    if input_string:
        start_args['input'] = input_string
    start_results = client.start_workflow_execution(**start_args)
    return start_results
