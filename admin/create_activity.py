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
        'defaultTaskScheduleToStartTimeout': '240',
        'defaultTaskScheduleToCloseTimeout': '240',
    }

    response = client.register_activity_type(**register_args)
    print(response)


if __name__ == '__main__':
    flows = [
        ('Leech', 'get_local_max_change_type_value', '1', 'retrieves the current maximum value for a given change_type by emp_id', 'Leech'),
        ('Leech', 'pull_change_types', '1', 'extracts all the currently listed Credible change_types', 'Leech'),
        ('Leech', 'get_local_ids', '3', 'queries id values in the data space', 'Leech'),
        ('Leech', 'put_new_ids', '1', 'adds id objects to the data space', 'Leech'),
        ('Leech', 'link_new_ids', '2', 'creates connection between ext_ids and a data source', 'Leech'),
        ('Leech', 'unlink_old_ids', '2', 'breaks the connection between ext_ids and a data source', 'Leech'),
        ('Leech', 'get_remote_ids', '3', 'retrieves all the remote id values for a given vertex driven process', 'Credible'),
        ('Leech', 'work_remote_id_change_type', '1', 'extract all the remote entries for a given change type for a given emp_id in Credible', 'Credible'),
    ]
    for flow in flows:
        try:
            register_activity(*flow)
        except ClientError as e:
            if e.response['Error']['Code'] != 'TypeAlreadyExistsFault':
                raise e
