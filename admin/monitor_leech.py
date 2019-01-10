import collections
from datetime import datetime, timedelta

import boto3

from toll_booth.alg_obj.aws.gentlemen.tasks import Versions


def get_open_workflows(domain_name, workflow_type, workflow_version):
    open_flows = []
    one_day_ago = datetime.utcnow() - timedelta(days=1)
    client = boto3.client('swf')
    paginator = client.get_paginator('list_open_workflow_executions')
    iterator = paginator.paginate(
        domain=domain_name,
        startTimeFilter={
            'oldestDate': one_day_ago
        },
        typeFilter={
            'name': workflow_type,
            'version': str(workflow_version)
        },
    )
    for page in iterator:
        for entry in page['executionInfos']:
            execution = entry['execution']
            parent_info = entry.get('parent', None)
            flow_info = {
                'run_id': execution['runId'],
                'flow_id': execution['workflowId']
            }
            if parent_info:
                flow_info['parent_flow_id'] = parent_info['workflowId']
                flow_info['parent_run_id'] = parent_info['runId']
            open_flows.append(flow_info)
    return open_flows


def get_workflow_history(domain_name, target_flow_id, target_run_id):
    history = []
    client = boto3.client('swf')
    paginator = client.get_paginator('get_workflow_execution_history')
    iterator = paginator.paginate(
        domain=domain_name,
        execution={
            'workflowId': target_flow_id,
            'runId': target_run_id
        }
    )
    for page in iterator:
        for event in page['events']:
            history.append(event)
    return history


def recurse_attach(target_key_value, attaching_obj, dict_obj):
    for key_entry, value_entry in dict_obj.items():
        if key_entry == target_key_value:
            update(value_entry, attaching_obj)
            return
        recurse_attach(target_key_value, attaching_obj, value_entry)


def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


if __name__ == '__main__':
    returned_flows = {}
    target_domain_name = 'Leech'
    versions = Versions.retrieve(target_domain_name)
    flow_names = ['fungus', 'command_fungi', 'work_remote_id', 'work_remote_id_change_type', 'work_remote_id_change_action']
    for flow_name in flow_names:
        current_open_flows = get_open_workflows(target_domain_name, flow_name, versions.workflow_versions[flow_name])
        for current_flow in current_open_flows:
            run_id = current_flow['run_id']
            flow_id = current_flow['flow_id']
            if 'parent_flow_id' not in current_flow:
                if flow_id not in returned_flows:
                    returned_flows[flow_id] = {}
                returned_flows[flow_id][run_id] = {}
                continue
            parent_flow_id = current_flow['parent_flow_id']
            parent_run_id = current_flow['parent_run_id']
            attached_obj = {parent_run_id: {flow_id: {run_id: {}}}}
            recurse_attach(parent_flow_id, attached_obj, returned_flows)
    print(returned_flows)
