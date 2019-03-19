from datetime import datetime, timedelta

import boto3
from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import RecordMarker, StartTimer, CancelTask
from toll_booth.alg_obj.aws.gentlemen.rafts import SubtaskSignature, group, ActivitySignature
from toll_booth.alg_tasks.rivers.rocks import workflow


# @xray_recorder.capture('ruffianing')
@workflow('ruffianing')
def ruffianing(**kwargs):
    existing_signatures = _build_existing_signatures(**kwargs)
    group_result = _build_group(existing_signatures, **kwargs)
    if group_result:
        great_group = group_result[0]
        great_group(**kwargs)
        return group_result[1]


def _find_new_ruffians(markers, signals, **kwargs):
    run_id = kwargs['run_id']
    completed_signals = markers.get_signal_markers(run_id)
    ruffian_signals = signals.ruffian_signals
    ruffian_signal_ids = set(x['signal_id'] for x in ruffian_signals)
    completed_ruffian_signal_ids = set(
        x['signal_id'] for x in completed_signals if x['signal_name'] in ['start_ruffian', 'stop_ruffian'])
    pending_ruffian_signal_ids = ruffian_signal_ids - completed_ruffian_signal_ids
    new_ruffians = []
    for ruffian_signal in ruffian_signals:
        ruffians = ruffian_signal['ruffians']
        flow_id = ruffian_signal['flow_id']
        signal_id = ruffian_signal['signal_id']
        signal_name = ruffian_signal['signal_name']
        for ruffian in ruffians:
            ruffian.update({
                'flow_id': flow_id, 'signal_id': signal_id,
                'signal_name': signal_name
            })
            if signal_id in pending_ruffian_signal_ids:
                new_ruffians.append(ruffian)
    return new_ruffians


def _build_group(existing_signatures, task_args, **kwargs):
    subtask_name = 'rough_housing'
    decisions = kwargs['decisions']
    running_flow_ids = [x.fn_identifier for x in existing_signatures]
    new_ruffians = _find_new_ruffians(**kwargs)
    sent_tasks = 0
    for ruffian in new_ruffians:
        flow_id = ruffian['flow_id']
        ruffian_id = ruffian['ruffian_id']
        cleaned_ruffian_id = ruffian_id.replace('#', '$')
        task_id = f'{flow_id}_{cleaned_ruffian_id}'
        if flow_id not in running_flow_ids:
            signal_name = ruffian['signal_name']
            if signal_name == 'start_ruffian':
                new_task_args = task_args.replace_argument_value(subtask_name, ruffian, task_id)
                kwargs.update({'task_args': new_task_args, 'running_time': 'NONE', 'heartbeat': 360})
                existing_signatures.append(ActivitySignature(task_id, subtask_name, **kwargs))
                running_flow_ids.append(flow_id)
                decisions.append(RecordMarker.for_signal_completed(ruffian['signal_id'], ruffian['signal_name']))
                sent_tasks += 1
            if signal_name == 'stop_ruffian':
                decisions.append(CancelTask(activity_id=task_id))
                decisions.append(RecordMarker.for_signal_completed(ruffian['signal_id'], ruffian['signal_name']))
    if existing_signatures:
        returned_group = group(*tuple(existing_signatures))
        return returned_group, sent_tasks


def _build_existing_signatures(task_args, **kwargs):
    operations = kwargs['activities']
    open_operations = operations.open_operations
    running_signatures = [
        ActivitySignature(
            x.operation_id, x.operation_name, x.task_args, register_results=False, **kwargs) for x in open_operations
    ]
    return running_signatures


def _query_running_flows(**kwargs):
    client = boto3.client('swf')
    paginator = client.get_paginator('list_open_workflow_executions')
    two_days_ago = datetime.utcnow() - timedelta(days=2)
    response_iterator = paginator.paginate(
        domain=kwargs['domain_name'],
        startTimeFilter={
            'oldestDate': two_days_ago
        },
        typeFilter={'name': 'rough_housing'}
    )
    running_flow_ids = []
    for page in response_iterator:
        running_flow_ids.extend([x['execution']['workflowId'] for x in page['executionInfos']])
    return running_flow_ids
