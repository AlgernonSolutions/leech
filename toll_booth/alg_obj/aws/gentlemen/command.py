from datetime import datetime, timedelta

import boto3

from toll_booth.alg_obj.aws.gentlemen.decisions import MadeDecisions, ScheduleLambda, CompleteWork
from toll_booth.alg_obj.aws.gentlemen.events.history import WorkflowHistory
from toll_booth.alg_tasks.rivers.flows.fungi import work_remote_id_change_action, command_fungi, work_remote_id, \
    work_remote_id_change_type


class General:
    def __init__(self, domain_name='Leech', task_list='Leech'):
        self._domain_name = domain_name
        self._task_list = task_list
        self._activity_tasks = []
        self._client = boto3.client('swf')

    def command(self):
        work_history = self._poll_for_decision()
        self._make_decisions(work_history)

    def _retrieve_run_work_history(self, flow_type, flow_id, run_id):
        workflow_histories = []
        history = None
        paginator = self._client.get_paginator('get_workflow_execution_history')
        response_iterator = paginator.paginate(
            domain=self._domain_name,
            execution={
                'workflowId': flow_id,
                'runId': run_id
            }
        )
        history_kwargs = {
            'flow_type': flow_type,
            'task_token': 'x',
            'flow_id': flow_id,
            'run_id': run_id
        }
        for page in response_iterator:
            workflow_history = WorkflowHistory.parse_from_poll(self._domain_name, page, **history_kwargs)
            workflow_histories.append(workflow_history)
        for workflow_history in workflow_histories:
            if history is None:
                history = workflow_history
                continue
            history.merge_history(workflow_history)
        return history

    def _retrieve_past_runs(self, flow_id):
        previous_runs = []
        paginator = self._client.get_paginator('list_closed_workflow_executions')
        one_day_ago = datetime.utcnow() - (timedelta(days=1))
        response_iterator = paginator.paginate(
            domain=self._domain_name,
            executionFilter={
                'workflowId': flow_id
            },
            startTimeFilter={
                'oldestDate': one_day_ago
            }
        )
        for page in response_iterator:
            for entry in page['executionInfos']:
                previous_runs.append(entry['execution']['runId'])
        return previous_runs

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
        past_runs = self._retrieve_past_runs(history.flow_id)
        for run_id in past_runs:
            run_history = self._retrieve_run_work_history(history.flow_type, history.flow_id, run_id)
            history.merge_history(run_history)
        return history

    def _make_decisions(self, work_history: WorkflowHistory):
        flow_modules = [
            command_fungi, work_remote_id, work_remote_id_change_type, work_remote_id_change_action
        ]
        if work_history:
            flow_type = work_history.flow_type
            for flow_module in flow_modules:
                flow = getattr(flow_module, flow_type, None)
                if flow:
                    return flow(work_history)
            raise NotImplementedError('could not find a registered flow for type: %s' % flow_type)

    def _run_lambda(self, fn_name: str, work_history: WorkflowHistory):
        made_decisions = MadeDecisions(work_history.task_token)
        flow_input = work_history.flow_input
        lambda_operation = work_history.get_lambda_operation(fn_name)
        if lambda_operation is None or (not lambda_operation.is_complete and not lambda_operation.is_live):
            made_decisions.add_decision(ScheduleLambda(fn_name, flow_input))
            self._client.respond_decision_task_completed(**made_decisions.for_commit)
            return
        lambda_results = lambda_operation.results
        if lambda_results:
            made_decisions.add_decision(CompleteWork(lambda_results))
            self._client.respond_decision_task_completed(**made_decisions.for_commit)
            return
        self._client.respond_decision_task_completed(**made_decisions.for_commit)
