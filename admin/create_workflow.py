import boto3
from botocore.exceptions import ClientError


def register_workflow(domain_name, flow_name, version, description, child_policy=None, task_list=None):
    client = boto3.client('swf')
    if not task_list:
        task_list = flow_name
    register_args = {
        'domain': domain_name,
        'name': flow_name,
        'version': str(version),
        'description': description,
        'defaultTaskStartToCloseTimeout': '240',
        'defaultExecutionStartToCloseTimeout': '240',
        'defaultTaskList': {
            'name': task_list
        },
        'defaultLambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
    if child_policy:
        register_args['defaultChildPolicy'] = child_policy
    response = client.register_workflow_type(**register_args)
    print(response)


if __name__ == '__main__':
    flows = [
        ('Leech', 'leech', '1', 'master level workflow for all typical leech operations', 'TERMINATE', 'Leech'),
        ('Leech', 'command_fungi', '1', 'orchestrates and runs the entire Credible FE extraction process', 'TERMINATE', 'Leech'),
        ('Leech', 'work_remote_id', '1', 'workflow for driving vertex growth from a single remote_id', 'TERMINATE', 'Leech'),
        ('Leech', 'work_remote_id_change_type', '1', 'workflow for extracting and collecting a change type', 'TERMINATE', 'Leech'),
        ('Leech', 'work_remote_id_change_action', '1', 'workflow for extracting and collecting a change action', 'TERMINATE', 'Leech'),
    ]
    for flow in flows:
        try:
            register_workflow(*flow)
        except ClientError as e:
            if e.response['Error']['Code'] != 'TypeAlreadyExistsFault':
                raise e
