import boto3
from botocore.exceptions import ClientError

from admin.refresh.tasks.task import _refresh_activity, _get_activities, _deprecate_activity
from admin.refresh.tasks.workflow import _get_workflows, _refresh_workflow, _deprecate_workflow
from toll_booth.alg_obj.aws.snakes.schema_snek import SchemaSnek


def refresh(domain_name, **kwargs):
    folder_name = kwargs.get('CONFIG_FOLDER', 'configs')
    config_file_name = kwargs.get('CONFIG_FILE', 'config.json')
    snek = SchemaSnek(folder_name=folder_name, **kwargs)
    current_config = snek.get_schema(config_file_name)
    _refresh_domain(domain_name, current_config)
    _refresh_workflows(domain_name, current_config)
    _refresh_activities(domain_name, current_config)
    print()


def _refresh_domain(domain_name, config):
    client = boto3.client('swf')
    domain_config = config['domain']
    try:
        client.describe_domain(
            name=domain_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'UnknownResourceFault':
            client.register_domain(
                name=domain_config['domain_name'],
                description=domain_config['description'],
                workflowExecutionRetentionPeriodInDays=str(domain_config['record_retention'])
            )
            return
        raise e


def _refresh_workflows(domain_name, config):
    workflow_configs = config['domain']['workflows']
    workflow_config_names = [x['workflow_name'] for x in workflow_configs]
    current_workflows = _get_workflows(domain_name)
    for workflow_config in workflow_configs:
        _refresh_workflow(domain_name, workflow_config, current_workflows)
    for current_workflow_name, current_workflow in current_workflows.items():
        if current_workflow_name not in workflow_config_names:
            for workflow_version in current_workflow:
                _deprecate_workflow(domain_name, current_workflow_name, workflow_version)


def _refresh_activities(domain_name, config):
    task_configs = config['domain']['tasks']
    task_config_names = [x['task_name'] for x in task_configs]
    current_activities = _get_activities(domain_name)
    for task_config in task_configs:
        _refresh_activity(domain_name, task_config, current_activities)
    for current_task_name, current_task in current_activities.items():
        if current_task_name not in task_config_names:
            for task_version in current_task:
                _deprecate_activity(domain_name, current_task_name, task_version)


if __name__ == '__main__':
    target_domain_name = 'Leech'
    refresh(target_domain_name)
