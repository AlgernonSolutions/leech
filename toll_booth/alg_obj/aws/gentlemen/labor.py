import json
from threading import Thread
from time import sleep

import boto3
from botocore.config import Config

from toll_booth.alg_obj.aws.gentlemen.tasks import Task
from toll_booth.alg_obj.serializers import AlgEncoder
from toll_booth.alg_tasks.rivers.tasks.fungi import fungi_tasks


class Laborer:
    def __init__(self, domain_name, task_list, laborer_name=None, context=None):
        self._laborer_name = laborer_name
        self._context = context
        self._domain_name = domain_name
        self._task_list = task_list
        self._client = boto3.client('swf', config=Config(
            connect_timeout=70, read_timeout=70, retries={'max_attempts': 0}))
        self._fire_alarm = False
        self._threads = []

    def labor(self):
        task = self._poll_for_activities()
        if task:
            labor_thread = Thread(name='labor', target=self._labor, kwargs={'task': task})
            heart_thread = Thread(name='heart', target=self._beat, kwargs={'task_token': task.task_token})
            self._threads.append(heart_thread)
            self._threads.append(labor_thread)
            for thread in self._threads:
                thread.start()
            for thread in self._threads:
                thread.join()

    def close_task(self, task_token: str, results: str):
        self._client.respond_activity_task_completed(
            taskToken=task_token,
            result=results
        )

    def poll_for_tasks(self):
        poll_args = {
            'domain': self._domain_name,
            'taskList': {'name': self._task_list}
        }
        if self._laborer_name:
            poll_args['identity'] = self._laborer_name
        response = self._client.poll_for_activity_task(**poll_args)
        return response

    def _labor(self, task):
        try:
            task_results = self._run_task(task)
            self._close_task(task, task_results)
        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            self._fail_task(task, e, trace)
        self._fire_alarm = True

    def _beat(self, task_token):
        while not self._fire_alarm:
            self._client.record_activity_task_heartbeat(
                taskToken=task_token
            )
            sleep(10)

    def _poll_for_activities(self):
        poll_args = {
            'domain': self._domain_name,
            'taskList': {'name': self._task_list}
        }
        if self._laborer_name:
            poll_args['identity'] = self._laborer_name
        response = self._client.poll_for_activity_task(**poll_args)
        return Task.parse_from_poll(response)

    def _run_task(self, task: Task):
        task_modules = [fungi_tasks]
        task_name = task.activity_name
        for task_module in task_modules:
            task_fn = getattr(task_module, task_name, None)
            if task_fn:
                results = task_fn(**task.task_args.for_task)
                return results
        raise NotImplementedError('could not find a registered task for type: %s' % task_name)

    def _close_task(self, task: Task, results: dict):
        result_string = json.dumps(results, cls=AlgEncoder)
        self._client.respond_activity_task_completed(
            taskToken=task.task_token,
            result=result_string
        )

    def _fail_task(self, task: Task, exception: Exception, trace):
        failure_reason = json.dumps(exception.args)
        failure_details = json.dumps({
            'task_name': task.activity_name,
            'task_version': task.activity_version,
            'trace': trace
        })
        self._client.respond_activity_task_failed(
            taskToken=task.task_token,
            reason=failure_reason,
            details=failure_details
        )
