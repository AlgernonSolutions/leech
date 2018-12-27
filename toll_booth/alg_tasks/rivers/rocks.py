import json

import boto3

from toll_booth.alg_obj.aws.gentlemen.decisions import MadeDecisions, CompleteWork
from toll_booth.alg_obj.serializers import AlgDecoder


def workflow(production_fn):
    def wrapper(*args):
        client = boto3.client('swf')
        work_history = args[0]
        flow_input = work_history.flow_input
        input_kwargs = json.loads(flow_input, cls=AlgDecoder)
        made_decisions = []
        context_kwargs = {
            'input_kwargs': input_kwargs,
            'flow_input': work_history.flow_input,
            'decisions': made_decisions,
            'work_history': work_history,
            'sub_tasks': work_history.subtask_operations,
            'markers': work_history.markers,
            'lambda_role': work_history.lambda_role,
            'execution_id': work_history.flow_id
        }
        results = production_fn(**context_kwargs)
        decisions = MadeDecisions(work_history.task_token)
        for decision in made_decisions:
            decisions.add_decision(decision)
        client.respond_decision_task_completed(**decisions.for_commit)
        if results == 'fire':
            client.signal_workflow_execution(
                domain=work_history.domain_name,
                workflowId=work_history.flow_id,
                runId=work_history.run_id,
                signalName='look_again'
            )
        return results
    return wrapper
