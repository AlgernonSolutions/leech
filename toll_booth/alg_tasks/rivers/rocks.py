import json

import boto3

from toll_booth.alg_obj.aws.gentlemen.decisions import MadeDecisions, CompleteWork, StartSubtask, RecordMarker
from toll_booth.alg_obj.aws.gentlemen.tasks import Versions, LeechConfig
from toll_booth.alg_obj.aws.ruffians.ruffian import RuffianRoost
from toll_booth.alg_obj.aws.snakes.snakes import StoredData


def _conscript_ruffian(work_history, start_subtask_decision, leech_config):
    workflow_config = leech_config.get_workflow_config(start_subtask_decision.type_name)
    labor_task_lists = workflow_config.get('labor_task_lists', [])
    labor_work_lists = {x['list_name']: x['number_threads'] for x in labor_task_lists}
    task_list_name = start_subtask_decision.type_name
    work_lists = {task_list_name: 1}
    work_lists.update(labor_work_lists)
    execution_arn = RuffianRoost.conscript_ruffians(task_list_name, work_lists, work_history.domain_name)
    start_subtask_decision.set_control(json.dumps({'execution_arn': execution_arn}))
    return RecordMarker.for_ruffian(start_subtask_decision.workflow_id, execution_arn)


def _disband_idle_ruffians(work_history):
    disband_markers = []
    idlers = work_history.idle_ruffians
    for execution_arn, operation_id in idlers.items():
        RuffianRoost.disband_ruffians(execution_arn)
        disband_markers.append(RecordMarker.for_ruffian(operation_id, execution_arn, True))
    return disband_markers


def workflow(workflow_name):
    def workflow_wrapper(production_fn):
        def wrapper(*args):
            client = boto3.client('swf')
            work_history = args[0]
            made_decisions = []
            versions = Versions.retrieve()
            configs = LeechConfig.get()
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
                'workflow_name': workflow_name,
                'workflow_args': task_args[workflow_name].data_string
            }
            results = production_fn(**context_kwargs)
            made_decisions.extend(_disband_idle_ruffians(work_history))
            decisions = MadeDecisions(work_history.task_token)
            for decision in made_decisions:
                if isinstance(decision, StartSubtask):
                    mark_ruffian = _conscript_ruffian(work_history, decision, configs)
                    decisions.add_decision(mark_ruffian)
                decisions.add_decision(decision)
            client.respond_decision_task_completed(**decisions.for_commit)
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
