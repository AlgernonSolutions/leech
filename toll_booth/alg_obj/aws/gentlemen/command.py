import json
import logging

import boto3
from botocore.client import Config
from retrying import retry

from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.aws.gentlemen.events.history import WorkflowHistory
from toll_booth.alg_obj.aws.gentlemen.events.markers import MarkerHistory
from toll_booth.alg_tasks.rivers.flows.fungi import work_remote_id_change_action, command_fungi, work_remote_id, \
    work_remote_id_change_type
from toll_booth.alg_tasks.rivers.flows import fungus


class General:
    def __init__(self, domain_name, task_list):
        self._domain_name = domain_name
        self._task_list = task_list
        self._activity_tasks = []
        self._client = boto3.client('swf', config=Config(connect_timeout=70))

    def command(self):
        work_history = self._poll_for_decision()
        if work_history is None:
            return
        try:
            self._make_decisions(work_history)
            logging.info(f'completed decision making on task_list: {self._task_list} for flow_id: {work_history.flow_id}')
        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            self._fail_task(work_history, e, trace)

    def _get_markers(self, flow_id, run_id):
        marker_histories = []
        history = None
        paginator = self._client.get_paginator('get_workflow_execution_history')
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

    def _get_past_runs(self, flow_id):
        from datetime import datetime, timedelta
        one_day_ago = datetime.utcnow() - timedelta(days=2)
        workflow_histories = []
        paginator = self._client.get_paginator('list_closed_workflow_executions')
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

    # @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=10000)
    def _poll_for_decision(self):
        history = None
        workflow_histories = []
        paginator = self._client.get_paginator('poll_for_decision_task')
        response_iterator = paginator.paginate(
            domain=self._domain_name,
            taskList={
                'name': self._task_list
            }
        )

        for page in response_iterator:
            if not page['taskToken']:
                return None
            logging.info(f'received a page in the decisions_iterator:" {page}')
            workflow_history = WorkflowHistory.parse_from_poll(self._domain_name, page)
            workflow_histories.append(workflow_history)
        for workflow_history in workflow_histories:
            if history is None:
                history = workflow_history
                continue
            history.merge_history(workflow_history)
        markers = self._get_past_runs(history.flow_id)
        for marker_history in markers:
            history.marker_history.merge_history(marker_history)
        return history

    def _make_decisions(self, work_history: WorkflowHistory):
        flow_modules = [
            command_fungi, work_remote_id, work_remote_id_change_type, work_remote_id_change_action, fungus
        ]
        if work_history:
            flow_type = work_history.flow_type
            for flow_module in flow_modules:
                flow = getattr(flow_module, flow_type, None)
                if flow:
                    return flow(work_history)
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
