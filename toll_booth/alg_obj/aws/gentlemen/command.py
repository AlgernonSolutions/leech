import json

import boto3
from retrying import retry

from toll_booth.alg_obj.aws.gentlemen.events.history import WorkflowHistory
from toll_booth.alg_tasks.rivers.flows.fungi import work_remote_id_change_action, command_fungi, work_remote_id, \
    work_remote_id_change_type
from toll_booth.alg_tasks.rivers.flows import fungus


class General:
    def __init__(self, domain_name='Leech', task_list='Leech'):
        self._domain_name = domain_name
        self._task_list = task_list
        self._activity_tasks = []
        self._client = boto3.client('swf')

    def command(self):
        work_history = self._poll_for_decision()
        try:
            self._make_decisions(work_history)
        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            self._fail_task(work_history, e, trace)

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_delay=10000)
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
            workflow_history = WorkflowHistory.parse_from_poll(self._domain_name, page)
            workflow_histories.append(workflow_history)
        for workflow_history in workflow_histories:
            if history is None:
                history = workflow_history
                continue
            history.merge_history(workflow_history)
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
