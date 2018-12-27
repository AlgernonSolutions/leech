import boto3

from toll_booth.alg_obj.aws.gentlemen.decisions import MadeDecisions, ScheduleLambda, StartSubtask, \
    CompleteWork
from toll_booth.alg_obj.aws.gentlemen.events import WorkflowHistory
from toll_booth.alg_tasks.rivers import fungi_flows


class General:
    def __init__(self, domain_name='Leech', task_list='Leech'):
        self._domain_name = domain_name
        self._task_list = task_list
        self._activity_tasks = []
        self._client = boto3.client('swf')

    def command(self):
        work_history = self._poll_for_decision()
        self._make_decisions(work_history)

    def execute(self):
        self._poll_for_activity()

    def _poll_for_activity(self):
        response = self._client.poll_for_activity_task(
            domain=self._domain_name,
            taskList={
                'name': 'Leech'
            }
        )
        print(response)

    def _poll_for_decision(self):
        workflow_history = None
        paginator = self._client.get_paginator('poll_for_decision_task')
        response_iterator = paginator.paginate(
            domain=self._domain_name,
            taskList={
                'name': self._task_list
            }
        )
        for page in response_iterator:
            if not workflow_history:
                workflow_history = WorkflowHistory.parse_from_poll(self._domain_name, page)
                continue
            workflow_history.add_events(page['events'])
        return workflow_history

    def _make_decisions(self, work_history: WorkflowHistory):
        flow_modules = [fungi_flows]
        if work_history:
            flow_type = work_history.flow_type
            for flow_module in flow_modules:
                flow = getattr(flow_module, flow_type, None)
                if flow:
                    return flow(work_history)
            raise NotImplementedError('could not find a registered flow for type: %s' % flow_type)

    def _command_fungi(self, work_history):
        made_decisions = MadeDecisions(work_history.task_token)
        flow_input = work_history.flow_input
        lambda_role = work_history.lambda_role
        execution_id = work_history.flow_id
        propagate_operation = work_history.get_subtask_operation(f'propagate-{execution_id}')
        creep_operation = work_history.get_subtask_operation(f'creep-{execution_id}')
        if propagate_operation is None or (not propagate_operation.is_complete and not propagate_operation.is_live):
            made_decisions.add_decision(StartSubtask(
                execution_id, 'propagate', flow_input, version='3',
                task_list_name='propagate', lambda_role=lambda_role
            ))
            self._client.respond_decision_task_completed(**made_decisions.for_commit)
            return
        propagation_results = propagate_operation.results
        if creep_operation is None or (not creep_operation.is_complete and not creep_operation.is_live):
            if propagation_results:
                made_decisions.add_decision(StartSubtask(
                    execution_id, 'creep', propagation_results, version='1',
                    task_list_name='creep', lambda_role=lambda_role
                ))
                self._client.respond_decision_task_completed(**made_decisions.for_commit)
                return
        self._client.respond_decision_task_completed(**made_decisions.for_commit)

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
