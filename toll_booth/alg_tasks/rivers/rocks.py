import logging

import boto3

from toll_booth.alg_obj.aws.continuum.q import Q
from toll_booth.alg_obj.aws.gentlemen.decisions import MadeDecisions, StartSubtask, RecordMarker, CompleteWork, \
    Decision, StartActivity
from toll_booth.alg_obj.aws.gentlemen.events.history import WorkflowHistory
from toll_booth.alg_obj.aws.gentlemen.tasks import Versions, LeechConfig
from toll_booth.alg_obj.aws.overseer.overseer import Overseer
from toll_booth.alg_obj.aws.ruffians.ruffian import RuffianRoost
from toll_booth.alg_obj.aws.snakes.snakes import StoredData


def _conscript_ruffian(decision: StartSubtask, work_history: WorkflowHistory, leech_config, versions):
    domain_name = work_history.domain_name
    overseer = Overseer.start(domain_name, versions)
    flow_id = decision.workflow_id
    flow_name = decision.type_name
    ruffians = RuffianRoost.generate_ruffians(domain_name, flow_id, flow_name, leech_config, {})
    return overseer.signal('start_ruffian', flow_id, leech_config, ruffians)


def _disband_ruffian(work_history: WorkflowHistory, leech_config, versions):
    domain_name = work_history.domain_name
    overseer = Overseer.start(domain_name, versions)
    flow_id = work_history.flow_id
    flow_name = work_history.flow_type
    ruffians = RuffianRoost.generate_ruffians(domain_name, flow_id, flow_name, leech_config, {})
    return overseer.signal('stop_ruffian', flow_id, leech_config, ruffians)


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
            configs = LeechConfig.retrieve()
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
                'domain_name': work_history.domain_name,
                'task_args': task_args,
                'run_id': work_history.run_id,
                'decisions': made_decisions,
                'work_history': work_history,
                'sub_tasks': work_history.subtask_history,
                'markers': work_history.marker_history,
                'signals': work_history.signal_history,
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
            decisions = MadeDecisions(work_history.task_token)
            if version_decision:
                decisions.add_decision(version_decision)
            if config_decision:
                decisions.add_decision(config_decision)
            for decision in made_decisions:
                if isinstance(decision, StartSubtask):
                    decision.set_parent_data(configs, versions)
                    _conscript_ruffian(decision, work_history, configs, versions)
                elif isinstance(decision, CompleteWork):
                    decision.add_result('task_args', task_args)
                    _disband_ruffian(work_history, configs, versions)
                elif isinstance(decision, StartActivity):
                    Q.send_task_reminder(decision.task_list, decision.type_name, decision.activity_id)
                try:
                    decision.set_id_info(work_history.flow_id, work_history.run_id)
                except AttributeError:
                    pass
                decisions.add_decision(decision)
            logging.info(f'sending the following decisions: {decisions.decisions}')
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
