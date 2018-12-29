import json
from threading import Thread
from time import sleep

import boto3

from toll_booth.alg_obj.aws.gentlemen.tasks import Task
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder
from toll_booth.alg_tasks.rivers.tasks.fungi import fungi_tasks


class Laborer:
    def __init__(self, laborer_name, domain_name='Leech', task_list='Leech'):
        self._laborer_name = laborer_name
        self._domain_name = domain_name
        self._task_list = task_list
        self._client = boto3.client('swf')
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

    def _labor(self, task):
        try:
            task_results = self._run_task(task)
            self._close_task(task, task_results)
        except Exception as e:
            self._fail_task(task, e)
        self._fire_alarm = True

    def _beat(self, task_token):
        while not self._fire_alarm:
            self._client.record_activity_task_heartbeat(
                taskToken=task_token
            )
            sleep(10)

    def _poll_for_activities(self):
        response = self._client.poll_for_activity_task(
            domain=self._domain_name,
            taskList={
                'name': self._task_list
            },
            identity=self._laborer_name
        )
        return Task.parse_from_poll(response)

    def _run_task(self, task: Task):
        task_modules = [fungi_tasks]
        task_name = task.activity_name
        for task_module in task_modules:
            task_fn = getattr(task_module, task_name, None)
            if task_fn:
                input_kwargs = json.loads(task.input_string, cls=AlgDecoder)
                results = task_fn(**input_kwargs)
                return results
        raise NotImplementedError('could not find a registered task for type: %s' % task_name)

    def _close_task(self, task: Task, results: dict):
        result_string = json.dumps(results, cls=AlgEncoder)
        self._client.respond_activity_task_completed(
            taskToken=task.task_token,
            result=result_string
        )

    def _fail_task(self, task: Task, exception: Exception):
        failure_details = json.dumps(exception.args)
        self._client.respond_activity_task_failed(
            taskToken=task.task_token,
            reason=str(exception),
            details=failure_details
        )
