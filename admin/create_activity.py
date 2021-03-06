import boto3
from botocore.exceptions import ClientError


def register_activity(domain_name, flow_name, version, description, task_list=None):
    client = boto3.client('swf')
    if not task_list:
        task_list = flow_name
    register_args = {
        'domain': domain_name,
        'name': flow_name,
        'version': str(version),
        'description': description,
        'defaultTaskStartToCloseTimeout': '240',
        'defaultTaskHeartbeatTimeout': '30',
        'defaultTaskList': {
            'name': task_list
        },
        'defaultTaskScheduleToStartTimeout': '3600',
        'defaultTaskScheduleToCloseTimeout': '3600',
    }

    response = client.register_activity_type(**register_args)
    print(response)


if __name__ == '__main__':
    flows = [
        ('Leech', 'get_local_max_change_type_value', '2', 'retrieves the current maximum value for a given change_type by emp_id', 'Leech'),
        ('Leech', 'build_mapping', '2', 'generates the mapping required to translate variously configured domains to a common standard', 'Leech'),
        ('Leech', 'pull_change_types', '2', 'extracts all the currently listed Credible change_types', 'Leech'),
        ('Leech', 'get_local_ids', '4', 'queries id values in the data space', 'Leech'),
        ('Leech', 'put_new_ids', '2', 'adds id objects to the data space', 'Leech'),
        ('Leech', 'link_new_ids', '3', 'creates connection between ext_ids and a data source', 'Leech'),
        ('Leech', 'unlink_old_ids', '3', 'breaks the connection between ext_ids and a data source', 'Leech'),
        ('Leech', 'get_remote_ids', '4', 'retrieves all the remote id values for a given vertex driven process', 'Credible'),
        ('Leech', 'work_remote_id_change_type', '2', 'extract all the remote entries for a given change type for a given emp_id in Credible', 'Credible'),
        ('Leech', 'get_enrichment_for_change_action', '2', 'if by_emp_id or change_details are needed for a given change_type, extract them', 'Credible'),
        ('Leech', 'generate_remote_id_change_data', '2', 'collects and formats the extracted data into a standard format recognizable by the Leech', 'Leech'),
    ]
    for flow in flows:
        try:
            register_activity(*flow)
        except ClientError as e:
            if e.response['Error']['Code'] != 'TypeAlreadyExistsFault':
                raise e
