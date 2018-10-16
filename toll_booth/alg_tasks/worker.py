import json
import logging

from aws_xray_sdk.core import xray_recorder


class Worker:
    """
    this is the container class for the logic required to run a function through lambda
    """
    @classmethod
    @xray_recorder.capture('work')
    def work(cls, event, context):
        """
        the actual working function, which is called during a work request
        the function loads the modules containing defined tasks and then attempts to find the task specified
        :param event: dict
            the standard input for an AWS lambda function, will contain all the
            information needed to execute the function
            task_args contains the information to be ultimately passed to the production function
            task_kwargs contains meta-information to configure the operation of the production function
            example: {'task_name': str, 'task_args': {}, 'task_kwargs': {}}
        :param context: dict
            the standard meta-data input for an AWS lambda function,
            used in this function to ascertain the amount of time remaining before timeout
        :return: obj
            on a successful operation, may or may not return the results of the operation, as a json serializable dict
            if no results are returned, returns True
            if an exception occurs, returns the exception as a string
            if no results are returned, and no exception is raised, the function may return an exit code or False
        """
        from toll_booth.alg_obj.serializers import AlgDecoder
        from toll_booth.alg_obj.serializers import AlgEncoder
        logging.info('received a lambda call to work a single task')
        logging.info(f'the provided event is:{event}')
        try:
            task_name = event['task_name']
        except KeyError:
            strung = json.dumps(event, cls=AlgEncoder)
            task_obj = json.loads(strung, cls=AlgDecoder)
            event = task_obj.to_json
            task_name = event['task_name']
        try:
            task_args = event['task_args']
        except KeyError:
            task_args = []
        try:
            worker_args = event['worker_args']
        except KeyError:
            worker_args = []
        try:
            task_constants = event['task_constants']
        except KeyError:
            task_constants = {}
        task_function = cls._find_function(task_name)
        results = task_function(task_args, worker_args=worker_args, task_constants=task_constants, context=context)
        return json.dumps(results, cls=AlgEncoder)

    @classmethod
    def _find_function(cls, task_name):
        from toll_booth.alg_tasks import remote_tasks, metl_tasks, find_tasks
        task_modules = [remote_tasks, metl_tasks, find_tasks]
        for task_unit in task_modules:
            try:
                task_module = getattr(task_unit, task_name)
                task_function = getattr(task_module, task_name)
                return task_function
            except AttributeError as e:
                logging.info(f'tried to find the task named {task_name} in module named {str(task_unit)}, to no avail')
                logging.info(e.args)
                continue
        raise NotImplementedError('remote task defined as: %s was not registered with the program' % task_name)
