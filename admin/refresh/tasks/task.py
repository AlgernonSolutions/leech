import logging

import boto3
from botocore.exceptions import ClientError

from admin.refresh.tasks import _compare_properties, _defaults


def _get_activities(domain_name):
    activities = {}
    client = boto3.client('swf')
    paginator = client.get_paginator('list_activity_types')
    iterator = paginator.paginate(
        domain=domain_name,
        registrationStatus='REGISTERED'
    )
    for page in iterator:
        for entry in page['typeInfos']:
            activity_type = entry['activityType']
            activity_name = activity_type['name']
            activity_version = activity_type['version']
            if activity_name not in activities:
                activities[activity_name] = {}
            activities[activity_name][activity_version] = _get_activity(domain_name, activity_name, activity_version)
    return activities


def _refresh_activity(domain_name, activity_config, current_activities):
    activity_name = activity_config['task_name']
    config_name = 'task_config'
    timeout_name = 'time_outs'
    config_properties = ['task_list']
    timeout_properties = ['running', 'waiting', 'heartbeat', 'total']

    try:
        current_activity = current_activities[activity_name]
    except KeyError:
        return _create_activity(domain_name, activity_config)
    max_version_number = max([int(x) for x in current_activity.keys()])
    max_version = current_activity[str(max_version_number)]
    new_version_number = str(max_version_number + 1)
    new_args = (domain_name, activity_config, new_version_number)
    if activity_config['task_description'] != max_version['task_description']:
        return _create_activity(*new_args)
    for config_property in config_properties:
        match = _compare_properties(activity_config, max_version, config_name, config_property)
        if not match:
            return _create_activity(*new_args)
    for time_out_property in timeout_properties:
        match = _compare_properties(activity_config, max_version, config_name, timeout_name, time_out_property)
        if not match:
            return _create_activity(*new_args)


def _get_activity(domain_name, activity_name, activity_version):
    client = boto3.client('swf')
    response = client.describe_activity_type(
        domain=domain_name,
        activityType={
            'name': activity_name,
            'version': activity_version
        }
    )
    activity_info = response['typeInfo']
    activity_type = activity_info['activityType']
    task_config = response['configuration']
    task_list = task_config.get('defaultTaskList', {})
    running_timeout = task_config.get('defaultTaskStartToCloseTimeout', _defaults['task_run_timeout'])
    total_timeout = task_config.get('defaultTaskScheduleToCloseTimeout', _defaults['task_total_timeout'])
    waiting_timeout = task_config.get('defaultTaskScheduleToStartTimeout', _defaults['task_waiting_timeout'])
    heartbeat_timeout = task_config.get('defaultTaskHeartbeatTimeout', _defaults['task_heart_timeout'])
    return {
        'task_name': activity_type['name'],
        'task_description': activity_info.get('description', None),
        'task_config': {
            'task_list': task_list.get('name', None),
            'time_outs': {
                'running': running_timeout,
                'total': total_timeout,
                'waiting': waiting_timeout,
                'heartbeat':heartbeat_timeout,
            }
        }
    }


def _create_activity(domain_name, task_config, version='1'):
    client = boto3.client('swf')
    config = task_config['task_config']
    time_outs = config.get('time_outs', {})
    register_args = {
        'domain': domain_name,
        'name': task_config['task_name'],
        'version': str(version),
        'description': task_config['task_description'],
        'defaultTaskStartToCloseTimeout': time_outs.get('running', _defaults['task_run_timeout']),
        'defaultTaskHeartbeatTimeout': time_outs.get('heartbeat', _defaults['task_heart_timeout']),
        'defaultTaskScheduleToStartTimeout': time_outs.get('waiting', _defaults['task_waiting_timeout']),
        'defaultTaskScheduleToCloseTimeout': time_outs.get('total', _defaults['task_total_timeout']),
    }
    if config.get('task_list', None):
        register_args['defaultTaskList'] = {'name': config['task_list']}
    try:
        client.register_activity_type(**register_args)
    except ClientError as e:
        if e.response['Error']['Code'] == 'TypeAlreadyExists':
            version = str(int(version) + 1)
            _create_activity(domain_name, task_config, version)
    logging.info(f'created a new task for {task_config["task_name"]}, version {version}')


def _deprecate_activity(domain_name, activity_name, activity_version):
    client = boto3.client('swf')
    client.deprecate_activity_type(
        domain=domain_name,
        activityType={
            'name': activity_name,
            'version': activity_version
        }
    )
    logging.info(f'deprecated a task for {activity_name}, version {activity_version}')
