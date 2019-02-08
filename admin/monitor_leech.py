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
            parent_info = entry.retrieve('parent', None)
            flow_info = {
                'run_id': execution['runId'],
                'flow_id': execution['workflowId']
            }
            if parent_info:
                flow_info['parent_flow_id'] = parent_info['workflowId']
                flow_info['parent_run_id'] = parent_info['runId']
            open_flows.append(flow_info)
    return open_flows


def get_completed_workflows(domain_name, workflow_type, workflow_version):
    open_flows = []
    one_day_ago = datetime.utcnow() - timedelta(days=1)
    client = boto3.client('swf')
    paginator = client.get_paginator('list_closed_workflow_executions')
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
            parent_info = entry.retrieve('parent', None)
            flow_info = {
                'run_id': execution['runId'],
                'flow_id': execution['workflowId']
            }
            if parent_info:
                flow_info['parent_flow_id'] = parent_info['workflowId']
                flow_info['parent_run_id'] = parent_info['runId']
            open_flows.append(flow_info)
    return open_flows


def get_workflow_history(domain_name, flow_id, run_id, event_filter=None):
    history = []
    client = boto3.client('swf')
    paginator = client.get_paginator('get_workflow_execution_history')
    iterator = paginator.paginate(
        domain=domain_name,
        execution={
            'workflowId': flow_id,
            'runId': run_id
        }
    )
    for page in iterator:
        for event in page['events']:
            if event_filter:
                if event['eventType'] != event_filter:
                    continue
            history.append(event)
    return history


def get_complete_flows(domain_name, flow_names):
    returned_flows = {}
    versions = Versions.retrieve(domain_name)
    for flow_name in flow_names:
        current_open_flows = get_completed_workflows(domain_name, flow_name, versions.workflow_versions[flow_name])
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
    return returned_flows


def get_checkpoint_history(domain_name, flow_id):
    markers = []
    run_ids = get_run_history(domain_name, flow_id=flow_id)
    for run_id in run_ids:
        collected_markers = get_workflow_history(domain_name, flow_id, run_id, 'MarkerRecorded')
        for marker in collected_markers:
            marker_name = marker['markerRecordedEventAttributes']['markerName']
        markers.extend(collected_markers)
    return markers


def get_run_history(domain_name, **kwargs):
    run_ids = set()
    one_day_ago = datetime.utcnow() - timedelta(days=1)
    client = boto3.client('swf')
    closed_paginator = client.get_paginator('list_closed_workflow_executions')
    open_paginator = client.get_paginator('list_open_workflow_executions')
    run_history_args = {
        'domain': domain_name,
        'startTimeFilter': {'oldestDate': one_day_ago}
    }
    if 'flow_name' in kwargs:
        versions = Versions.retrieve(domain_name)
        flow_name = kwargs['flow_name']
        flow_version = versions.workflow_versions[flow_name]
        run_history_args['typeFilter'] = {'name': flow_name, 'version': str(flow_version)}
    if 'flow_id' in kwargs:
        run_history_args['executionFilter'] = {'workflowId': kwargs['flow_id']}
    closed_iterator = closed_paginator.paginate(**run_history_args)
    for page in closed_iterator:
        for entry in page['executionInfos']:
            execution = entry['execution']
            run_id = execution['runId']
            run_ids.add(run_id)
    open_iterator = open_paginator.paginate(**run_history_args)
    for page in open_iterator:
        for entry in page['executionInfos']:
            execution = entry['execution']
            run_id = execution['runId']
            run_ids.add(run_id)
    return run_ids


def recurse_attach(target_key_value, attaching_obj, dict_obj):
    for key_entry, value_entry in dict_obj.items():
        if key_entry == target_key_value:
            update(value_entry, attaching_obj)
            return
        recurse_attach(target_key_value, attaching_obj, value_entry)


def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            d[k] = update(d.retrieve(k, {}), v)
        else:
            d[k] = v
    return d


if __name__ == '__main__':
    target_domain_name = 'TheLeech'
    target_flow_names = ['fungus', 'command_fungi', 'work_remote_id', 'work_remote_id_change_type', 'work_remote_id_change_action']
    get_checkpoint_history(target_domain_name, '31')

