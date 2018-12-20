import boto3


def register_workflow(domain_name, flow_name, version, description, child_policy=None):
    client = boto3.client('swf')
    register_args = {
        'domain': domain_name,
        'name': flow_name,
        'version': str(version),
        'description': description,
        'defaultTaskStartToCloseTimeout': '120',
        'defaultExecutionStartToCloseTimeout': '120',
        'defaultTaskList': {
            'name': flow_name
        },
        'defaultLambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
    if child_policy:
        register_args['defaultChildPolicy'] = child_policy
    response = client.register_workflow_type(**register_args)
    print(response)


if __name__ == '__main__':
    register_workflow('Leech', 'creep', '1', 'populates change types for a given vertex type', 'TERMINATE')