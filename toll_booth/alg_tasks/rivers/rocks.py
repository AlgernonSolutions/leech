import boto3

from toll_booth.alg_obj.aws.gentlemen.decisions import MadeDecisions, RecordMarker
from toll_booth.alg_obj.aws.gentlemen.tasks import Versions, LeechConfig
from toll_booth.alg_obj.aws.snakes.snakes import StoredData


def workflow(workflow_name):
    def workflow_wrapper(production_fn):
        def wrapper(*args):
            client = boto3.client('swf')
            work_history = args[0]
            made_decisions = []
            versions = Versions.retrieve()
            configs = LeechConfig.get()
            names = work_history.marker_history.names
            task_args = work_history.task_args
            context_kwargs = {
                'task_args': task_args,
                'decisions': made_decisions,
                'work_history': work_history,
                'sub_tasks': work_history.subtask_history,
                'markers': work_history.marker_history,
                'timers': work_history.timer_history,
                'activities': work_history.activity_history,
                'lambda_role': work_history.lambda_role,
                'execution_id': work_history.flow_id,
                'versions': versions,
                'configs': configs,
                'names': names,
                'workflow_name': workflow_name,
                'workflow_args': task_args[workflow_name]
            }
            results = production_fn(**context_kwargs)
            decisions = MadeDecisions(work_history.task_token)
            decisions.add_decision(RecordMarker.for_names(context_kwargs['names']))
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
    return workflow_wrapper


def task(task_name):
    def task_wrapper(production_fn):
        def wrapper(**kwargs):
            results = production_fn(**kwargs)
            if results is None:
                return results
            stored_results = StoredData.from_object(task_name, results, full_unpack=False)
            return stored_results
        return wrapper
    return task_wrapper
