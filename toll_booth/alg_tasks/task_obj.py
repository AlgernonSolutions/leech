import json
import logging
import os
from datetime import datetime

from toll_booth.alg_obj.serializers import AlgDecoder


class InsufficientOperationTimeException(Exception):
    def __init__(self, completed_results):
        self._completed_results = completed_results


class QueuedTaskParameters:
    """
    a parsing class used to derive the environment from metadata passed to the function
    """

    def __init__(self, source_queue_url, target_queue_url, num_messages, wait_time, ignore_results):
        """
        constructor
        :param source_queue_url:
            if the arguments for the function are in an SQS queue, the url will be here, None if not source queued
        :param target_queue_url:
            if the function results should be pushed to a queue, url here, None if results are not queued
        :param num_messages:
            the number of messages to pull from the source queue for a single execution of the production function
        :param wait_time:
            how long to wait for messages fro the source queue before giving up
        :param ignore_results:
            discard the results of the function
        """
        self._source_queue_url = source_queue_url
        self._target_queue_url = target_queue_url
        self._num_messages = num_messages
        self._wait_time = wait_time
        self._ignore_results = ignore_results

    @classmethod
    def parse(cls, param_dict):
        try:
            source_queue_url = param_dict['source_queue_url']
        except KeyError:
            source_queue_url = None
        try:
            target_queue_url = param_dict['target_queue_url']
        except KeyError:
            target_queue_url = None
        try:
            num_messages = param_dict['num_messages']
        except KeyError:
            num_messages = 10
        try:
            wait_time = param_dict['wait_time']
        except KeyError:
            wait_time = 1
        try:
            ignore_results = param_dict['ignore_results']
        except KeyError:
            ignore_results = True
        return cls(source_queue_url, target_queue_url, num_messages, wait_time, ignore_results)

    @property
    def is_fully_queued(self):
        if self.has_target_queue and self.has_source_queue:
            return True
        return False

    @property
    def is_only_queue_sourced(self):
        if self.has_source_queue and not self.has_target_queue:
            return True
        return False

    @property
    def is_only_target_queued(self):
        if not self.has_source_queue and self.has_target_queue:
            return True
        return False

    @property
    def has_source_queue(self):
        if self._source_queue_url:
            return True
        return False

    @property
    def source_queue_url(self):
        return self._source_queue_url

    @property
    def target_queue_url(self):
        return self._target_queue_url

    @property
    def has_target_queue(self):
        if self._target_queue_url:
            return True
        return False

    @property
    def num_messages(self):
        return self._num_messages

    @property
    def wait_time(self):
        return self._wait_time

    @property
    def ignore_results(self):
        return self._ignore_results

    @property
    def as_worker_args(self):
        return {
            'target_queue_url': self._target_queue_url,
            'source_queue_url': self._source_queue_url,
            'num_messages': self._num_messages,
            'wait_time': self._wait_time,
            'ignore_results': self._ignore_results
        }


def remote_task(production_function):
    """
    the decorator class for functions designed to be run remotely by AWS lambda
    this wrapper allows the production function logic to remain constant, while the environment
    can be changed to accommodate different use cases
    :param production_function: fn
        the actual function to be performed remotely
    :return: object
        depending on the environment, could be None, True, False, or json serializable results
    """

    def wrapper(*args, **kwargs):
        """
        within the wrapper, we configure the environment the function will operate in
        currently, this is used to configure the source of the function arguments/keywords,
        as well as to specify what to do with the results
        :param args: tuple
            if the function should be fed directly by the invoker, and the results returned directly,
            all arguments should be here
        :param kwargs: {}
            if the function should be fed by a queue, should have it's results pushed to a queue, or both,
            the functions arguments, as well as the required metadata will be here
            example: {'worker_args': {information about queues}, 'task_params': {constants for the function}}
        :return: object
            the results of the production function
        """
        args = args[0]
        context = kwargs['context']
        worker_args = kwargs.get('worker_args', None)
        task_constants = kwargs.get('task_constants', None)
        try:
            args = json.loads(args, cls=AlgDecoder)
        except TypeError:
            pass
        return production_function(
            task_args=args,
            task_constants=task_constants,
            worker_args=worker_args,
            context=context
        )

    return wrapper


def metered(production_function):
    def wrapper(*args, **kwargs):
        context = kwargs['context']
        logging.info('working a metered task')
        start = datetime.now()
        results = production_function(*args, **kwargs)
        end = datetime.now()
        running_time = (end - start).seconds * 1000
        run_times = os.getenv('run_times')
        if not run_times:
            run_times = []
        else:
            run_times = json.loads(run_times)
        run_times.append(running_time)
        os.environ['run_times'] = json.dumps(run_times)
        time_left = context.get_remaining_time_in_millis()
        average_run_time = sum(run_times) / float(len(run_times))
        if time_left < 10 * average_run_time:
            logging.info('ran out of time before the ask was completed')
            raise InsufficientOperationTimeException(results)
        logging.info('completed the metered task')
        return results

    return wrapper
