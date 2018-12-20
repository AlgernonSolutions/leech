import boto3

from toll_booth.alg_obj.aws.gentlemen.decisions import DecisionParameters, MadeDecisions, ScheduleLambda, StartSubtask, \
    CompleteWork
from toll_booth.alg_obj.aws.gentlemen.events import LambdaEventSets, SubtaskEventSets


class General:
    def __init__(self, domain_name='Leech', task_list='Leech'):
        self._domain_name = domain_name
        self._task_list = task_list
        self._decision_parameters = DecisionParameters.generate_empty()
        self._activity_tasks = []
        self._client = boto3.client('swf')

    def command(self):
        self._poll_for_decision()
        self._make_decisions()

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
        decision = None
        paginator = self._client.get_paginator('poll_for_decision_task')
        response_iterator = paginator.paginate(
            domain=self._domain_name,
            taskList={
                'name': self._task_list
            }
        )
        for page in response_iterator:
            if not decision:
                decision = DecisionParameters.parse_from_decision_poll(page)
                continue
            decision.add_additional_events(page['events'])
        if decision:
            self._decision_parameters = decision
            return True
        return False

    def _make_decisions(self):
        if self._decision_parameters:
            flow_type = self._decision_parameters.flow_type
            if flow_type == 'CredibleFeLeech':
                return self._command_fungi()
            if flow_type == 'propagate':
                return self._run_lambda('leech-propagate')
            if flow_type == 'creep':
                return self._run_lambda('leech-creep')

    def _command_fungi(self):
        made_decisions = MadeDecisions(self._decision_parameters.task_token)
        start_work = self._decision_parameters.event_history.get_event_by_type('WorkflowExecutionStarted')
        flow_input = start_work[0].event_attributes['input']
        lambda_role = start_work[0].event_attributes['lambdaRole']
        execution_id = self._decision_parameters.flow_id
        sub_tasks = self._decision_parameters.event_history.get_subtask_sets()
        if not sub_tasks:
            made_decisions.add_decision(StartSubtask(
                execution_id, 'propagate', flow_input, version='3',
                task_list_name='propagate', lambda_role=lambda_role
            ))
            self._client.respond_decision_task_completed(**made_decisions.for_commit)
            return
        propagation_results = self._check_subtask_completion('propagate', sub_tasks)
        if propagation_results:
            made_decisions.add_decision(StartSubtask(
                execution_id, 'creep', flow_input, version='3',
                task_list_name='creep', lambda_role=lambda_role
            ))
            self._client.respond_decision_task_completed(**made_decisions.for_commit)
            return
        self._client.respond_decision_task_completed(**made_decisions.for_commit)

    def _propagate(self):
        made_decisions = MadeDecisions(self._decision_parameters.task_token)
        start_work = self._decision_parameters.event_history.get_event_by_type('WorkflowExecutionStarted')
        flow_input = start_work[0].event_attributes['input']
        lambda_tasks = self._decision_parameters.event_history.get_lambda_sets()
        if not lambda_tasks:
            made_decisions.add_decision(ScheduleLambda('leech-propagate', flow_input))
            self._client.respond_decision_task_completed(**made_decisions.for_commit)
            return
        propagation_results = self._check_lambda_completion('leech-propagate', lambda_tasks)
        if propagation_results:
            made_decisions.add_decision(CompleteWork(propagation_results))
            self._client.respond_decision_task_completed(**made_decisions.for_commit)
            return
        self._client.respond_decision_task_completed(**made_decisions.for_commit)

    def _run_lambda(self, fn_name):
        made_decisions = MadeDecisions(self._decision_parameters.task_token)
        start_work = self._decision_parameters.event_history.get_event_by_type('WorkflowExecutionStarted')
        flow_input = start_work[0].event_attributes['input']
        lambda_tasks = self._decision_parameters.event_history.get_lambda_sets()
        if not lambda_tasks:
            made_decisions.add_decision(ScheduleLambda(fn_name, flow_input))
            self._client.respond_decision_task_completed(**made_decisions.for_commit)
            return
        propagation_results = self._check_lambda_completion(fn_name, lambda_tasks)
        if propagation_results:
            made_decisions.add_decision(CompleteWork(propagation_results))
            self._client.respond_decision_task_completed(**made_decisions.for_commit)
            return
        self._client.respond_decision_task_completed(**made_decisions.for_commit)

    @classmethod
    def _check_subtask_completion(cls, sub_task_name: str, subtask_sets: SubtaskEventSets):
        task_sets = subtask_sets.get_for_task_name(sub_task_name)
        if not task_sets:
            return False
        for entry in task_sets:
            if entry.is_completed:
                return entry.results
        return False

    @classmethod
    def _check_lambda_completion(cls, fn_name: str, lambda_tasks: LambdaEventSets):
        propagation_sets = lambda_tasks.get_sets_by_name(fn_name)
        if not propagation_sets:
            return False
        for entry in propagation_sets:
            if entry.is_completed:
                return entry.results
        return False
