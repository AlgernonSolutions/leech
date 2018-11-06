import json

from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder


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
        if worker_args:
            worker_params = QueuedTaskParameters.parse(worker_args)
            if worker_params.is_fully_queued:
                return fully_queued(production_function,
                                    task_constants=task_constants,
                                    worker_params=worker_params,
                                    context=context
                                    )
            if worker_params.is_only_queue_sourced:
                return queue_sourced(production_function,
                                     task_constants=task_constants,
                                     worker_params=worker_params,
                                     context=context
                                     )
            if worker_params.is_only_target_queued:
                return queue_targeted(production_function,
                                      task_args=args,
                                      task_constants=task_constants,
                                      worker_params=worker_params,
                                      context=context
                                      )
        try:
            args = json.loads(args, cls=AlgDecoder)
        except TypeError:
            pass
        return production_function(task_args=args,
                                   task_constants=task_constants,
                                   worker_args=worker_args,
                                   context=context
                                   )
    return wrapper


def fully_queued(production_function, task_constants, worker_params, context):
    """
    environmental setup for functions that are fed from a queue, and return their results to a queue,
    retrieves a batch of messages from the source queue, runs the function on each message,
    collects the results and pushes them to the target queue
    :param production_function: fn()
        the function to be executed
    :param task_constants:
        constants for the function, which do not need to be repeated in the queue
    :param worker_params: QueuedTaskParameters
        environmental configurations
    :param context: dict
        standard metadata from AWS about the function, used to monitor timeout
    :return: object
        returns True if the function runs successfully and the results sent to the queue
        returns Exception as string if the function fails
    """
    from toll_booth.alg_obj.aws.aws_obj import MessagePull, OutboundMessage
    from toll_booth.alg_obj.aws.matryoshkas.message_swarm import MessageSwarm
    message_pull = MessagePull.get_from_queue(
        num_messages=worker_params.num_messages, wait_time=worker_params.wait_time,
        queue_url=worker_params.source_queue_url)
    swarm = MessageSwarm(worker_params.target_queue_url)
    if not message_pull:
        return False
    for message in message_pull:
        print('got some messages from the queue, working on one')
        task_args = json.loads(message.message_body, cls=AlgDecoder)
        try:
            print('trying out one of the messages')
            results = production_function(
                task_args=task_args, task_constants=task_constants,
                worker_args=worker_params.as_worker_args, context=context
            )
            print('got the results of the message: %s ' % results)
        except Exception as e:
            import traceback
            traceback.print_exc()
            return 'exception while executing remote function: %s' % e
        try:
            for result in results:
                swarm.add_message(OutboundMessage(json.dumps(result, cls=AlgEncoder)))
        except TypeError:
            swarm.add_message(OutboundMessage(json.dumps(results, cls=AlgEncoder)))
        message.mark_completed()
    swarm.send()
    return True


def queue_targeted(production_function, task_args, task_constants, worker_params, context):
    from toll_booth.alg_obj.aws.aws_obj import OutboundMessage
    from toll_booth.alg_obj.aws.matryoshkas.message_swarm import MessageSwarm
    swarm = MessageSwarm(worker_params.target_queue_url)
    try:
        results = production_function(
            task_args=task_args, task_constants=task_constants,
            worker_args=worker_params.as_worker_args, context=context)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return 'exception while executing remote function: %s' % e
    try:
        for result in results:
            swarm.add_message(OutboundMessage(json.dumps(result, cls=AlgEncoder)))
    except TypeError:
        swarm.add_message(OutboundMessage(json.dumps(results, cls=AlgEncoder)))
    swarm.send()
    return False


def queue_sourced(production_function, task_constants, worker_params, context):
    from toll_booth.alg_obj.aws.aws_obj import MessagePull
    message_pull = MessagePull.get_from_queue(
        num_messages=worker_params.num_messages, wait_time=worker_params.wait_time,
        queue_url=worker_params.source_queue_url)
    if not message_pull:
        return False
    results = []
    for message in message_pull:
        task_args = json.loads(message.message_body, cls=AlgDecoder)
        results.append(production_function(
            task_args=task_args, task_constants=task_constants,
            worker_args=worker_params.as_worker_args, context=context))
        message.mark_completed()
    if worker_params.ignore_results:
        return True
    return json.dumps(results, cls=AlgEncoder)
