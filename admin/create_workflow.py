import boto3


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
    register_workflow('Leech', 'pull_change_types', '1', 'retrieves the current list of remote change types for Credible', 'TERMINATE', 'Leech')
