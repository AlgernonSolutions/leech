import logging

import boto3

from toll_booth.alg_obj.aws.gentlemen.decisions import MadeDecisions, StartSubtask, RecordMarker
from toll_booth.alg_obj.aws.gentlemen.tasks import Versions, LeechConfig
from toll_booth.alg_obj.aws.ruffians.ruffian import RuffianRoost
from toll_booth.alg_obj.aws.snakes.snakes import StoredData


def _conscript_ruffian(work_history, start_subtask_decision, leech_config):
    workflow_id = start_subtask_decision.workflow_id
    open_ruffians = work_history.marker_history.get_open_ruffian_tasks(work_history.run_id)
    if workflow_id in open_ruffians:
        return
    workflow_config = leech_config.get_workflow_config(start_subtask_decision.type_name)
    labor_task_lists = workflow_config.get('labor_task_lists', [])
    execution_arns = RuffianRoost.conscript_ruffians(workflow_id, labor_task_lists, work_history.domain_name)
    return [RecordMarker.for_ruffian(workflow_id, x) for x in execution_arns]


def _disband_idle_ruffians(work_history):
    disband_markers = []
    idlers = work_history.idle_ruffians
    for execution_arn, operation_id in idlers.items():
        RuffianRoost.disband_ruffians(execution_arn)
        disband_markers.append(RecordMarker.for_ruffian(operation_id, execution_arn, True))
    return disband_markers


def _get_versions(work_history):
    versions = work_history.versions
    marker = work_history.marker_history.versions_marker
    if versions is None:
        if marker is None:
            versions = Versions.retrieve(work_history.domain_name)
            return versions, RecordMarker.for_versions(versions)
        return marker.marker_details, None
    return versions, None


def _get_config(work_history):
    config = work_history.config
    marker = work_history.marker_history.config_marker
    if config is None:
        if marker is None:
            configs = LeechConfig.get()
            return configs, RecordMarker.for_config(configs)
        return marker.marker_details, None
    return config, None


def _set_run_id_logging(flow_id, run_id, context):
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging.basicConfig(format='[%(levelname)s] || ' +
                               f'function_name:{context.function_name}|function_arn:{context.invoked_function_arn}'
                               f'|request_id:{context.aws_request_id}' +
                               f'|flow_id:{flow_id}|run_id:{run_id} || %(asctime)s %(message)s', level=logging.INFO)


def workflow(workflow_name):
    def workflow_wrapper(production_fn):
        def wrapper(*args, **kwargs):
            client = boto3.client('swf')
            work_history = args[0]
            if kwargs['context']:
                _set_run_id_logging(work_history.flow_id, work_history.run_id, kwargs['context'])
            made_decisions = []
            versions, version_decision = _get_versions(work_history)
            configs, config_decision = _get_config(work_history)
            task_args = work_history.task_args
            workflow_args = task_args.get(workflow_name, {})
            if workflow_args:
                workflow_args = workflow_args.data_string
            context_kwargs = {
                'task_args': task_args,
                'decisions': made_decisions,
                'work_history': work_history,
                'sub_tasks': work_history.subtask_history,
                'markers': work_history.marker_history,
                'timers': work_history.timer_history,
                'activities': work_history.activity_history,
                'lambdas': work_history.lambda_history,
                'lambda_role': work_history.lambda_role,
                'execution_id': work_history.flow_id,
                'versions': versions,
                'configs': configs,
                'workflow_name': workflow_name,
                'workflow_args': workflow_args
            }
            results = production_fn(**context_kwargs)
            made_decisions.extend(_disband_idle_ruffians(work_history))
            decisions = MadeDecisions(work_history.task_token)
            if version_decision:
                decisions.add_decision(version_decision)
            if config_decision:
                decisions.add_decision(config_decision)
            for decision in made_decisions:
                if isinstance(decision, StartSubtask):
                    decision.set_parent_data(configs, versions)
                    mark_ruffian = _conscript_ruffian(work_history, decision, configs)
                    if mark_ruffian:
                        for marker in mark_ruffian:
                            decisions.add_decision(marker)
                try:
                    decision.set_id_info(work_history.flow_id, work_history.run_id)
                except AttributeError:
                    pass
                decisions.add_decision(decision)
            client.respond_decision_task_completed(**decisions.for_commit)
            return results
        return wrapper
    return workflow_wrapper


def task(task_name):
    def task_wrapper(production_fn):
        def wrapper(**kwargs):
            logging.info(f'starting task: {task_name}, kwargs: {kwargs}')
            results = production_fn(**kwargs)
            logging.info(f'completed task: {task_name}, results: {results}')
            if results is None:
                return results
            stored_results = StoredData.from_object(task_name, results, full_unpack=False)
            return stored_results
        return wrapper
    return task_wrapper
