import json
import logging
import random

from toll_booth.alg_obj.aws.gentlemen.decisions import StartActivity, StartTimer, StartSubtask, RecordMarker, \
    StartLambda
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder


class ConcurrencyExceededException(Exception):
    def __init__(self, identifier):
        self._message = f'unable to start signature: {identifier} as max_concurrency reached'


class Signature:
    def __init__(self, name, version, config, identifier, **kwargs):
        self._fn_name = name
        self._fn_version = version
        self._fn_identifier = identifier
        self._config = config
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
        if self._back_off_status is True:
            return
        if self._is_complete:
            self._set_checkpoint(**kwargs)
            return self._results
        if self._is_failed:
            back_off_count = self._back_off_status
            if self._fail_count != back_off_count:
                self._back_off(**kwargs)
                return
            self._start(**kwargs)
            return
        if self._is_started and not self._is_failed:
            return
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

    def _start(self, **kwargs):
        decisions = kwargs['decisions']
        self._check_concurrency(**kwargs)
        start_operation = self._build_start(**kwargs)
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
        self._set_checkpoint(**kwargs)
        results = self._results
        if results is not None:
            results = json.loads(self._results, cls=AlgDecoder)
        signature_results = {self._fn_name: results}
        kwargs['task_args'].add_argument_values(signature_results)
        return signature_results


class SubtaskSignature(Signature):
    def __init__(self, subtask_identifier, subtask_type, task_list=None, **kwargs):
        if not task_list:
            task_list = subtask_identifier
        version = getattr(kwargs['versions'], 'workflow_versions')[subtask_type]
        config = kwargs['configs'][('workflow', subtask_type)]
        cls_kwargs = self.generate_signature_status(subtask_identifier, kwargs['sub_tasks'], **kwargs)
        super().__init__(subtask_type, version, config, subtask_identifier, **cls_kwargs)
        self._task_list = task_list

    def _build_start(self, task_args, **kwargs):
        return StartSubtask(*self.start_args, task_args=task_args, version=self.fn_version, task_list=self._task_list, **kwargs)

    def _check_concurrency(self, **kwargs):
        operations = kwargs['sub_tasks']
        operation_type = 'StartChildWorkflowExecution'
        super()._check_concurrency(operations, operation_type, **kwargs)


class ActivitySignature(Signature):
    def __init__(self, task_identifier, task_type, task_list=None, **kwargs):
        config = kwargs['configs'][('task', task_type)]
        if not task_list:
            task_list = config.get('task_list', kwargs['work_history'].flow_id)
        version = getattr(kwargs['versions'], 'task_versions')[task_type]
        cls_kwargs = self.generate_signature_status(task_identifier, kwargs['activities'], **kwargs)
        super().__init__(task_type, version, config, task_identifier, **cls_kwargs)
        self._task_list = task_list

    def _build_start(self, task_args, **kwargs):
        return StartActivity(*self.start_args, task_args=task_args, version=self.fn_version, task_list=self._task_list)

    def _check_concurrency(self, **kwargs):
        operations = kwargs['activities']
        operation_type = 'ScheduleActivityTask'
        super()._check_concurrency(operations, operation_type, **kwargs)


class LambdaSignature(Signature):
    def __init__(self, lambda_identifier, task_type, **kwargs):
        version = getattr(kwargs['versions'], 'task_versions')[task_type]
        config = kwargs['configs'][('task', task_type)]
        cls_kwargs = self.generate_signature_status(lambda_identifier, kwargs['lambdas'], **kwargs)
        super().__init__(task_type, version, config, lambda_identifier, **cls_kwargs)
        self._control = kwargs.get('control', None)

    def _build_start(self, task_args, **kwargs):
        return StartLambda(*self.start_args, task_args=task_args, control=self._control)

    def _check_concurrency(self, **kwargs):
        operations = kwargs['lambdas']
        operation_type = 'LambdaFunctionScheduled'
        super()._check_concurrency(operations, operation_type, **kwargs)


class Chain:
    def __init__(self, signatures):
        self._signatures = signatures

    def __call__(self, *args, task_args, **kwargs):
        chain_results = {}
        for signature in self._signatures:
            if not signature.is_started:
                signature(task_args, **kwargs)
                return
            if signature.is_failed:
                signature(task_args, **kwargs)
                return
            if not signature.is_complete and not signature.is_failed:
                return
            results = signature.get_results(**kwargs)
            chain_results = results
            task_args.add_argument_values(results)
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
        results = {}
        for signature in self._signatures:
            results.update(signature.get_results(**kwargs))
        return results

    def __call__(self, *args, task_args, **kwargs):
        group_results = {}
        group_started = True
        group_finished = True
        for signature in self._signatures:
            if not signature.is_started:
                try:
                    signature(task_args, **kwargs)
                except ConcurrencyExceededException:
                    logging.warning(f'reached maximum concurrency running group, will retry as tasks finish')
                    return
                group_started = False
        if not group_started:
            return
        for signature in self._signatures:
            if signature.is_failed:
                signature(task_args, **kwargs)
                group_finished = False
                continue
            if not signature.is_complete and not signature.is_failed:
                group_finished = False
                continue
            results = signature.get_results(**kwargs)
            task_args.add_argument_values(results)
            group_results.update(results)
        if not group_finished:
            return
        return group_results


def chain(*args):
    return Chain(args)


def group(*args):
    return Group(args)
