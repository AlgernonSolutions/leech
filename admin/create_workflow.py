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
        ('Leech', 'command_fungi', '1', 'orchestrates and runs the entire Credible FE extraction process', 'TERMINATE', 'Leech'),
        ('Leech', 'get_remote_ids', '1', 'extracts id_values from a Credible FE domain', 'TERMINATE', 'Leech'),
        ('Leech', 'get_local_ids', '1', 'queries id values in the data space', 'TERMINATE', 'Leech'),
        ('Leech', 'put_new_ids', '1', 'adds id objects to the data space', 'TERMINATE', 'Leech'),
        ('Leech', 'link_new_ids', '1', 'creates connection between ext_ids and a data source', 'TERMINATE', 'Leech'),
        ('Leech', 'unlink_old_ids', '1', 'breaks the connection between ext_ids and a data source', 'TERMINATE', 'Leech'),
        ('Leech', 'pull_change_types', '1', 'retrieves the current list of remote change types for Credible', 'TERMINATE', 'Leech'),
        ('Leech', 'get_local_max_change_type_value', '1', 'current maximum value for a change_type category per emp_id', 'TERMINATE', 'Leech'),
        ('Leech', 'work_remote_id_change_type', '1', 'workflow for extracting and collecting a change type', 'TERMINATE', 'Leech'),
        ('Leech', 'work_remote_id_change_type', '1', 'workflow for extracting and collecting a change type', 'TERMINATE', 'Leech'),
        ('Leech', 'get_remote_id_by_emp_ids', '1', 'enriches change log data by retrieving the by_emp_id', 'TERMINATE', 'Leech'),
        ('Leech', 'get_remote_id_change_details', '1', 'enriches change log data by retrieving the change details', 'TERMINATE', 'Leech'),
        ('Leech', 'generate_remote_id_change_data', '1', 'aggregates the collected and enriched data together', 'TERMINATE', 'Leech'),
        ('Leech', 'put_remote_id_change_data', '1', 'stores the collected data', 'TERMINATE', 'Leech'),
    ]
    for flow in flows:
        try:
            register_workflow(*flow)
        except ClientError as e:
            if e.response['Error']['Code'] != 'TypeAlreadyExistsFault':
                raise e
