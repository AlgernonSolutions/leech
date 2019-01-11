import boto3

from admin.refresh.tasks import _compare_properties, _defaults


def _get_workflows(domain_name):
    workflows = {}
    client = boto3.client('swf')
    paginator = client.get_paginator('list_workflow_types')
    iterator = paginator.paginate(
        domain=domain_name,
        registrationStatus='REGISTERED'
    )
    for page in iterator:
        for entry in page['typeInfos']:
            workflow_type = entry['workflowType']
            flow_name = workflow_type['name']
            flow_version = workflow_type['version']
            if flow_name not in workflows:
                workflows[flow_name] = {}
            workflows[flow_name][flow_version] = _get_workflow(domain_name, flow_name, flow_version)
    return workflows


def _get_workflow(domain_name, workflow_name, workflow_version):
    client = boto3.client('swf')
    response = client.describe_workflow_type(
        domain=domain_name,
        workflowType={
            'name': workflow_name,
            'version': workflow_version
        }
    )
    flow_info = response['typeInfo']
    flow_type = flow_info['workflowType']
    flow_config = response['configuration']
    task_list = flow_config.get('defaultTaskList', {})
    workflow_timeout = flow_config.get('defaultExecutionStartToCloseTimeout', None)
    task_timeout = flow_config.get('defaultTaskStartToCloseTimeout', None)
    if workflow_timeout:
        workflow_timeout = int(workflow_timeout)
    if task_timeout:
        task_timeout = int(task_timeout)
    return {
        'workflow_name': flow_type['name'],
        'workflow_description': flow_info.get('description', None),
        'workflow_config': {
            'decision_task_list': task_list.get('name', None),
            'time_outs': {
                'workflow': workflow_timeout,
                'task': task_timeout
            },
            'lambda_role': flow_config.get('defaultLambdaRole', None),
            'child_policy': flow_config.get('defaultChildPolicy', None)
        }
    }


def _refresh_workflow(domain_name, config_workflow, current_workflows):
    flow_name = config_workflow['workflow_name']
    config_name = 'workflow_config'
    timeout_name = 'time_outs'
    config_properties = ['decision_task_list', 'lambda_role', 'child_policy']
    timeout_properties = ['workflow', 'task']

    try:
        current_workflow = current_workflows[flow_name]
    except KeyError:
        return _create_workflow(domain_name, config_workflow)
    max_version_number = max([int(x) for x in current_workflow.keys()])
    max_version = current_workflow[str(max_version_number)]
    new_version_number = str(max_version_number + 1)
    new_args = (domain_name, config_workflow, new_version_number)
    if config_workflow['workflow_description'] != max_version['workflow_description']:
        return _create_workflow(*new_args)
    for config_property in config_properties:
        match = _compare_properties(config_workflow, max_version, config_name, config_property)
        if not match:
            return _create_workflow(*new_args)
    for time_out_property in timeout_properties:
        match = _compare_properties(config_workflow, max_version, config_name, timeout_name, time_out_property)
        if not match:
            return _create_workflow(*new_args)


def _create_workflow(domain_name, flow_config, version='1'):
    client = boto3.client('swf')
    config = flow_config['workflow_config']
    time_outs = config.get('time_outs', {})
    register_args = {
        'domain': domain_name,
        'name': flow_config['workflow_name'],
        'version': str(version),
        'description': flow_config['workflow_description'],
        'defaultTaskStartToCloseTimeout': str(time_outs.get('task', _defaults['workflow_timeout'])),
        'defaultExecutionStartToCloseTimeout': str(time_outs.get('workflow', _defaults['workflow_task_timeout'])),
        'defaultLambdaRole': config.get('lambda_role', _defaults['lambda_role']),
        'defaultChildPolicy': config.get('child_policy', _defaults['child_policy'])
    }
    if config.get('decision_task_list', None):
        register_args['defaultTaskList'] = {'name': config['decision_task_list']}
    client.register_workflow_type(**register_args)


def _deprecate_workflow(domain_name, workflow_name, workflow_version):
    client = boto3.client('swf')
    client.deprecate_workflow_type(
        domain=domain_name,
        workflowType={
            'name': workflow_name,
            'version': workflow_version
        }
    )
