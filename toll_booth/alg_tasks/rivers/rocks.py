import json
from json import JSONDecodeError

import boto3

from toll_booth.alg_obj.aws.gentlemen.decisions import MadeDecisions
from toll_booth.alg_obj.aws.gentlemen.tasks import Versions
from toll_booth.alg_obj.aws.snakes.snakes import StoredData
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder, TaskDecoder


def workflow(production_fn):
    def wrapper(*args):
        client = boto3.client('swf')
        work_history = args[0]
        flow_input = work_history.flow_input
        input_kwargs = json.loads(flow_input, cls=AlgDecoder)
        made_decisions = []
        versions = Versions.retrieve()
        context_kwargs = {
            'input_kwargs': input_kwargs,
            'flow_input': work_history.flow_input,
            'decisions': made_decisions,
            'work_history': work_history,
            'sub_tasks': work_history.subtask_history,
            'markers': work_history.marker_history,
            'activities': work_history.activity_history,
            'lambda_role': work_history.lambda_role,
            'execution_id': work_history.flow_id,
            'versions': versions
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


def task(task_name):
    def task_wrapper(production_fn):
        def wrapper(**kwargs):
            formatted_data = {}
            names = kwargs['names']
            del(kwargs['names'])
            strung_kwargs = json.dumps(kwargs, cls=AlgEncoder)
            formatted_kwargs = json.loads(strung_kwargs, cls=TaskDecoder)
            results = production_fn(**formatted_kwargs)
            if results is None:
                return results
            stored_results = StoredData.from_object(task_name, results, full_unpack=False)
            return stored_results
        return wrapper
    return task_wrapper
