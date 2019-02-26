import json
import logging

import boto3
from botocore.client import Config
from botocore.exceptions import ConnectionClosedError, ClientError
from retrying import retry

from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.aws.gentlemen.events.history import WorkflowHistory
from toll_booth.alg_obj.aws.gentlemen.events.markers import MarkerHistory
from toll_booth.alg_tasks.rivers.flows.fungi import work_remote_id_change_action, command_fungi, work_remote_id, \
    work_remote_id_change_type, work_fip_links, post_process_encounters
from toll_booth.alg_tasks.rivers.flows import fungus
from toll_booth.alg_tasks.rivers.flows.leech import fungal_leech
from toll_booth.alg_tasks.rivers.flows.posts import send_routine_reports


class WorkHistoryRetrievalException(Exception):
    def __init__(self, work_history):
        self._work_history = work_history

    @property
    def work_history(self):
        return self._work_history


class General:
    def __init__(self, domain_name, task_list, context=None, identity=None):
        self._domain_name = domain_name
        self._task_list = task_list
        self._context = context
        self._identity = identity
        self._client = boto3.client('swf', config=Config(
            connect_timeout=70, read_timeout=70, retries={'max_attempts': 0}))

    def command(self):
        try:
            work_history = self._poll_for_decision()
        except ConnectionClosedError:
            work_history = None
        except WorkHistoryRetrievalException as e:
            import traceback
            trace = traceback.format_exc()
            self._fail_task(e.work_history, e, trace)
            return
        if work_history is None:
            return
        try:
            self._make_decisions(work_history)
            logging.info(f'completed decision making on task_list: {self._task_list} for flow_id: {work_history.flow_id}')
        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            self._fail_task(work_history, e, trace)

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=10000)
    def _get_markers(self, flow_id, run_id):
        marker_histories = []
        history = None
        client = boto3.client('swf', config=Config(retries={'max_attempts': 5}))
        paginator = client.get_paginator('get_workflow_execution_history')
        response_iterator = paginator.paginate(
            domain=self._domain_name,
            execution={
                'workflowId': flow_id,
                'runId': run_id
            }
        )
        for page in response_iterator:
            events = [Event.parse_from_decision_poll_event(x) for x in page['events']]
            marker_history = MarkerHistory.generate_from_events(run_id, events)
            marker_histories.append(marker_history)
        for marker_history in marker_histories:
            if history is None:
                history = marker_history
                continue
            history.merge_history(marker_history)
        return history

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=10000)
    def _get_past_runs(self, flow_id):
        from datetime import datetime, timedelta
        one_day_ago = datetime.utcnow() - timedelta(days=2)
        workflow_histories = []
        client = boto3.client('swf', config=Config(retries={'max_attempts': 5}))
        paginator = client.get_paginator('list_closed_workflow_executions')
        response_iterator = paginator.paginate(
            domain=self._domain_name,
            executionFilter={
                'workflowId': flow_id
            },
            closeTimeFilter={
                'oldestDate': one_day_ago,
            }
        )
        for page in response_iterator:
            for entry in page['executionInfos']:
                run_id = entry['execution']['runId']
                flow_history = self._get_markers(flow_id, run_id)
                workflow_histories.append(flow_history)
        return workflow_histories

    def _poll_for_decision(self):
        events = []
        history = None
        paginator = self._client.get_paginator('poll_for_decision_task')
        poll_args = {
            'domain': self._domain_name,
            'taskList': {'name': self._task_list}
        }
        if self._identity:
            poll_args['identity'] = self._identity
        response_iterator = paginator.paginate(**poll_args)

        for page in response_iterator:
            if 'taskToken' not in page:
                return None
            logging.info(f'received a page in the decisions_iterator: {page}')
            events.extend(page['events'])
            history = page
        history['events'] = events
        workflow_history = WorkflowHistory.parse_from_poll(self._domain_name, history)
        try:
            markers = self._get_past_runs(workflow_history.flow_id)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ThrottlingException':
                raise e
            raise WorkHistoryRetrievalException(workflow_history)
        for marker_history in markers:
            workflow_history.marker_history.merge_history(marker_history)
        return workflow_history

    def _make_decisions(self, work_history: WorkflowHistory):
        flow_modules = [
            command_fungi, work_remote_id, work_remote_id_change_type,
            work_remote_id_change_action, fungus, fungal_leech, send_routine_reports,
            work_fip_links, post_process_encounters
        ]
        if work_history:
            flow_type = work_history.flow_type
            for flow_module in flow_modules:
                flow = getattr(flow_module, flow_type, None)
                if flow:
                    return flow(work_history, context=self._context)
            raise NotImplementedError('could not find a registered flow for type: %s' % flow_type)

    def _fail_task(self, work_history, exception, trace):
        failure_reason = json.dumps(exception.args)
        failure_details = json.dumps({
            'workflow_name': work_history.flow_type,
            'trace': trace
        })
        self._client.terminate_workflow_execution(
            domain=work_history.domain_name,
            workflowId=work_history.flow_id,
            runId=work_history.run_id,
            reason=failure_reason,
            details=failure_details,
            childPolicy='TERMINATE'
        )
