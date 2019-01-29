import json
import logging
import random

from toll_booth.alg_obj.aws.gentlemen.decisions import StartActivity, StartTimer, StartSubtask, RecordMarker, \
    StartLambda
from toll_booth.alg_obj.aws.gentlemen.tasks import TaskArguments
from toll_booth.alg_obj.serializers import AlgDecoder


class ConcurrencyExceededException(Exception):
    def __init__(self, identifier):
        self._message = f'unable to start signature: {identifier} as max_concurrency reached'


class Signature:
    def __init__(self, name, version, config, identifier, task_args: TaskArguments = None, **kwargs):
        self._fn_name = name
        self._fn_version = version
        self._fn_identifier = identifier
        self._config = config
        self._task_args = task_args
        self._is_started = kwargs['is_started']
        self._is_complete = kwargs['is_complete']
        self._is_failed = kwargs['is_failed']
        self._fail_count = kwargs['fail_count']
        self._back_off_status = kwargs['back_off_status']
        self._results = kwargs['results']

    @classmethod
    def generate_signature_status(cls, identifier, operations, **kwargs):
        cls_kwargs = {
            'is_started': False, 'is_complete': False,
            'is_failed': False, 'fail_count': 0,
            'back_off_status': False, 'results': None
        }
        checkpoint = cls._check_for_checkpoint(identifier, **kwargs)
        if checkpoint:
            cls_kwargs.update({
                'is_complete': True, 'results': checkpoint, 'is_started': True
            })
            return cls_kwargs
        if identifier not in operations.operation_ids:
            return cls_kwargs
        operation = operations[identifier]
        results = operation.results
        fail_count = operations.get_operation_failed_count(identifier)
        cls_kwargs.update({
            'is_started': True, 'is_complete': operation.is_complete,
            'is_failed': operation.is_failed, 'fail_count': fail_count,
            'back_off_status': kwargs['timers'].fn_back_off_status(identifier),
            'results': results
        })
        return cls_kwargs

    @classmethod
    def _check_for_checkpoint(cls, identifier, **kwargs):
        markers = kwargs['markers']
        checkpoints = markers.checkpoints
        if identifier in checkpoints:
            return checkpoints[identifier]
        return None

    def __call__(self, *args, **kwargs):
        task_args = kwargs['task_args']
        logging.info(f'calling a signature, name: {self._fn_name}, args: {args}, task_args: {task_args}, kwargs: {kwargs}')
        if self._back_off_status is True:
            logging.info(f'signature {self._fn_name} is backing off due to failures')
            return
        if self._is_complete:
            logging.info(f'signature {self._fn_name} is completed, results: {self._results}')
            results = self.get_results(**kwargs)
            task_args.add_argument_values(results)
            return self._results
        if self._is_failed:
            logging.info(f'signature {self._fn_name} has failed')
            back_off_count = self._back_off_status
            logging.info(f'signature {self._fn_name} has back_off_count: {back_off_count}')
            if self._fail_count != back_off_count:
                logging.info(f'based on the back_off_count: {back_off_count} and the fail_count: {self._fail_count}, '
                             f'signature: {self._fn_name} needs to be backed off')
                self._back_off(**kwargs)
                return
            logging.info(f'signature {self._fn_name} has been backed off already, can be rerun now, invoking_kwargs: {kwargs}')
            self._start(**kwargs)
            return
        if self._is_started and not self._is_failed:
            logging.info(f'signature: {self._fn_name} is has started, and has not failed, let it run')
            return
        logging.info(f'signature: {self._fn_name} is ready to be run, starting it')
        self._start(**kwargs)

    def _back_off(self, **kwargs):
        decisions = kwargs['decisions']
        back_off = round((2 ** self._fail_count) + (random.randint(0, 1000) / 1000))
        warning_message = 'activity signature for: %s, has failed %s times, will keep retrying ' \
                          'but implementing back off logic [%s s]' % (self._fn_identifier, self._fail_count, back_off)
        logging.warning(warning_message)
        details = {
            'fn_identifier': self._fn_identifier,
            'fn_name': self._fn_name,
            'fail_count': self._fail_count
        }
        start_timer = StartTimer('error_back_off', back_off, details)
        decisions.append(start_timer)

    def _start(self, task_args, **kwargs):
        decisions = kwargs['decisions']
        self._check_concurrency(**kwargs)
        start_operation = self._build_start(task_args=task_args, **kwargs)
        decisions.append(start_operation)

    def _build_start(self, **kwargs):
        raise NotImplementedError()

    def _check_concurrency(self, operations, concurrent_type, **kwargs):
        decisions = kwargs['decisions']
        pending_run = [x for x in decisions if x.decision_type == concurrent_type]
        current_concurrency = len([x for x in operations if x.is_live and x.operation_name == self._fn_name])
        pending_concurrency = len([x for x in pending_run if x.type_name == self._fn_name])
        target_concurrency = self._config['concurrency']
        if (current_concurrency + pending_concurrency) > target_concurrency:
            raise ConcurrencyExceededException(self._fn_identifier)

    def _set_checkpoint(self, **kwargs):
        markers = kwargs['markers']
        checkpoints = markers.checkpoints
        if self._fn_identifier not in checkpoints and self._is_complete:
            kwargs['decisions'].append(
                RecordMarker.for_checkpoint(self._fn_identifier, self._results)
            )

    @property
    def start_args(self):
        start_args = (self._fn_identifier, self._fn_name)
        return start_args

    @property
    def fn_name(self):
        return self._fn_name

    @property
    def fn_identifier(self):
        return self._fn_identifier

    @property
    def fn_version(self):
        return str(self._fn_version)

    @property
    def is_started(self):
        return self._is_started

    @property
    def is_failed(self):
        return self._is_failed

    @property
    def fail_count(self):
        return self._fail_count

    @property
    def is_complete(self):
        return self._is_complete

    def get_results(self, **kwargs):
        logging.info(f'called get_results on signature: {self}')
        self._set_checkpoint(**kwargs)
        results = self._results
        logging.info(f'signature: {self} has results: {results}')
        if results is not None:
            results = json.loads(self._results, cls=AlgDecoder)
        signature_results = {self._fn_name: results}
        logging.info(f'after serializing and compiling the signature_results are: {signature_results}')
        kwargs['task_args'].add_argument_values(signature_results)
        return signature_results

    def __str__(self):
        return self._fn_name


class SubtaskSignature(Signature):
    def __init__(self, subtask_identifier, subtask_type, task_args: TaskArguments = None, task_list=None, **kwargs):
        if not task_list:
            task_list = subtask_identifier
        version = getattr(kwargs['versions'], 'workflow_versions')[subtask_type]
        config = kwargs['configs'][('workflow', subtask_type)]
        cls_kwargs = self.generate_signature_status(subtask_identifier, kwargs['sub_tasks'], **kwargs)
        super().__init__(subtask_type, version, config, subtask_identifier, task_args, **cls_kwargs)
        self._task_list = task_list

    def _build_start(self, task_args: TaskArguments, **kwargs):
        if self._task_args:
            task_args.merge_other_task_arguments(self._task_args)
        return StartSubtask(*self.start_args, task_args=task_args, version=self.fn_version, task_list=self._task_list, **kwargs)

    def _check_concurrency(self, **kwargs):
        operations = kwargs['sub_tasks']
        operation_type = 'StartChildWorkflowExecution'
        super()._check_concurrency(operations, operation_type, **kwargs)


class ActivitySignature(Signature):
    def __init__(self, task_identifier, task_type, task_args: TaskArguments = None, task_list=None, **kwargs):
        config = kwargs['configs'][('task', task_type)]
        if not task_list:
            task_list = config.get('task_list', kwargs['work_history'].flow_id)
        version = getattr(kwargs['versions'], 'task_versions')[task_type]
        cls_kwargs = self.generate_signature_status(task_identifier, kwargs['activities'], **kwargs)
        super().__init__(task_type, version, config, task_identifier, task_args, **cls_kwargs)
        self._task_list = task_list

    def _build_start(self, task_args: TaskArguments, **kwargs):
        if self._task_args:
            task_args.merge_other_task_arguments(self._task_args)
        return StartActivity(*self.start_args, task_args=task_args, version=self.fn_version, task_list=self._task_list)

    def _check_concurrency(self, **kwargs):
        operations = kwargs['activities']
        operation_type = 'ScheduleActivityTask'
        super()._check_concurrency(operations, operation_type, **kwargs)


class LambdaSignature(Signature):
    def __init__(self, lambda_identifier, task_type, task_args: TaskArguments = None, **kwargs):
        version = getattr(kwargs['versions'], 'task_versions')[task_type]
        config = kwargs['configs'][('task', task_type)]
        cls_kwargs = self.generate_signature_status(lambda_identifier, kwargs['lambdas'], **kwargs)
        super().__init__(task_type, version, config, lambda_identifier, task_args, **cls_kwargs)
        self._control = kwargs.get('control', None)

    def _build_start(self, task_args: TaskArguments, **kwargs):
        if self._task_args:
            task_args.merge_other_task_arguments(self._task_args)
        return StartLambda(*self.start_args, task_args=task_args, control=self._control)

    def _check_concurrency(self, **kwargs):
        operations = kwargs['lambdas']
        operation_type = 'LambdaFunctionScheduled'
        super()._check_concurrency(operations, operation_type, **kwargs)


class Chain:
    def __init__(self, signatures):
        self._signatures = signatures

    def get_results(self, **kwargs):
        logging.info(f'calling the get_results method on a chain with signatures: {self._signatures}, invoking_kwargs: {kwargs}')
        results = {}
        for signature in self._signatures:
            results = signature.get_results(**kwargs)
        logging.info(f'the returned results for the chain with signatures: {self._signatures} are {results}')
        return results

    def __call__(self, *args, **kwargs):
        task_args = kwargs['task_args']
        logging.info(f'calling a chain with args: {args}, task_args: {task_args}, kwargs: {kwargs}')
        chain_results = {}
        for signature in self._signatures:
            logging.info(f'starting a signature within a chain: {signature}')
            if not signature.is_started:
                logging.info(f'signature: {signature} is not started yet, so let us start it')
                signature(**kwargs)
                return
            if signature.is_failed:
                logging.info(f'signature: {signature} has failed, let us retry it')
                signature(**kwargs)
                return
            if not signature.is_complete and not signature.is_failed:
                logging.info(f'signature: {signature} is neither failed nor completed, hopefully running, we will check later')
                return
            results = signature.get_results(**kwargs)
            logging.info(f'signature: {signature} has completed with results: {results}')
            chain_results = results
            task_args.add_argument_values(results)
        logging.info(f'the chain has completed, chain_results: {chain_results}')
        return chain_results


class Group:
    def __init__(self, signatures):
        self._signatures = signatures

    @property
    def is_started(self):
        for signature in self._signatures:
            if not signature.is_started:
                return False
        return True

    @property
    def is_failed(self):
        for signature in self._signatures:
            if signature.is_failed:
                return True
        return False

    @property
    def fail_count(self):
        fail_count = 0
        for signature in self._signatures:
            if signature.is_failed:
                fail_count += signature.fail_count
        return fail_count

    @property
    def is_complete(self):
        for signature in self._signatures:
            if not signature.is_complete:
                return False
        return True

    def get_results(self, **kwargs):
        logging.info(f'called get results on a group')
        results = {}
        for signature in self._signatures:
            results.update(signature.get_results(**kwargs))
        logging.info(f'group results are: {results}')
        return results

    def __call__(self, *args, **kwargs):
        task_args = kwargs['task_args']
        logging.info(f'called a group, args: {args}, task_args: {task_args}, kwargs: {kwargs}')
        group_results = {}
        group_started = True
        group_finished = True
        logging.info(f'checking to see which signatures in the group are running currently')
        for signature in self._signatures:
            if not signature.is_started:
                logging.info(f'signature: {signature} is not running, let us start it')
                try:
                    signature(**kwargs)
                except ConcurrencyExceededException:
                    logging.warning(f'reached maximum concurrency running group, will retry as tasks finish')
                    return
                group_started = False
        if not group_started:
            logging.info(f'not all the signatures in a group had been started, so no point checking the results yet')
            return
        for signature in self._signatures:
            if signature.is_failed:
                logging.info(f'signature: {signature} in a group has failed, retry it')
                signature(**kwargs)
                group_finished = False
                continue
            if not signature.is_complete and not signature.is_failed:
                logging.info(f'signature: {signature} is not complete, nor failed, hopefully running, come back later')
                group_finished = False
                continue
            results = signature.get_results(**kwargs)
            task_args.add_argument_values(results)
            group_results.update(results)
        if not group_finished:
            logging.info(f'some signatures in the group had not completed, do not check results yet')
            return
        logging.info(f'all the signatures in this group are completed, return the results: {group_results}')
        return group_results


def chain(*args):
    return Chain(args)


def group(*args):
    return Group(args)
