import json
import logging
import random

from toll_booth.alg_obj.aws.gentlemen.decisions import StartActivity, StartTimer, StartSubtask
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder


class Signature:
    def __init__(self, name, version, identifier, signature_kwargs, **kwargs):
        self._fn_name = name
        self._fn_version = version
        self._fn_identifier = identifier
        self._fn_kwargs = signature_kwargs
        self._is_activity = kwargs['is_activity']
        self._is_started = kwargs['is_started']
        self._is_complete = kwargs['is_complete']
        self._is_failed = kwargs['is_failed']
        self._fail_count = kwargs['fail_count']
        self._back_off_status = kwargs['back_off_status']
        self._results = kwargs['results']

    @classmethod
    def for_activity(cls, fn_identifier, fn_name, fn_kwargs=None, **kwargs):
        is_activity = True
        if not fn_kwargs:
            fn_kwargs = {}
        activities = kwargs['activities']
        timers = kwargs['timers']
        versions = kwargs['versions']
        fn_version = versions.task_versions[fn_name]
        cls_args = (fn_name, fn_version, fn_identifier, fn_kwargs)
        cls_kwargs = {
            'is_activity': is_activity, 'is_started': False, 'is_complete': False,
            'is_failed': False, 'fail_count': 0, 'back_off_status': False, 'results': None
        }
        if fn_identifier not in activities.operation_ids:
            return cls(*cls_args, **cls_kwargs)
        activity_operation = activities[fn_identifier]
        results = activity_operation.results
        fail_count = activities.get_operation_failed_count(fn_identifier)
        cls_kwargs.update({
            'is_started': True, 'is_complete': activity_operation.is_complete,
            'is_failed': activity_operation.is_failed, 'fail_count': fail_count,
            'back_off_status': timers.fn_back_off_status(fn_identifier),
            'results': results
        })
        return cls(*cls_args, **cls_kwargs)

    @classmethod
    def for_subtask(cls, subtask_identifier, subtask_name, subtask_kwargs=None, **kwargs):
        is_activity = False
        if not subtask_kwargs:
            subtask_kwargs = {}
        sub_tasks = kwargs['sub_tasks']
        timers = kwargs['timers']
        versions = kwargs['versions']
        subtask_version = versions.workflow_versions[subtask_name]
        cls_args = (subtask_name, subtask_version, subtask_identifier, subtask_kwargs)
        cls_kwargs = {
            'is_activity': is_activity, 'is_started': False, 'is_complete': False,
            'is_failed': False, 'fail_count': 0, 'back_off_status': False, 'results': None
        }
        if subtask_identifier not in sub_tasks.operation_ids:
            return cls(*cls_args, **cls_kwargs)
        subtask_operation = sub_tasks[subtask_identifier]
        results = subtask_operation.results
        fail_count = sub_tasks.get_operation_failed_count(subtask_identifier)
        cls_kwargs.update({
            'is_started': True, 'is_complete': subtask_operation.is_complete,
            'is_failed': subtask_operation.is_failed, 'fail_count': fail_count,
            'back_off_status': timers.fn_back_off_status(subtask_identifier),
            'results': results
        })
        return cls(*cls_args, **cls_kwargs)

    def __call__(self, *args, **kwargs):
        if self._back_off_status is True:
            return
        if self._is_complete:
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
            'fn_kwargs': self._fn_kwargs,
            'fail_count': self._fail_count
        }
        start_timer = StartTimer('error_back_off', back_off, details)
        decisions.append(start_timer)

    def _start(self, **kwargs):
        decisions = kwargs['decisions']
        start_operation = self._build_start(**kwargs)
        decisions.append(start_operation)

    def _build_start(self, **kwargs):
        task_args = kwargs['task_args']
        task_args.add_arguments({self._fn_name: self._fn_kwargs})
        flow_input = json.dumps(task_args, cls=AlgEncoder)
        activity_args = (self._fn_identifier, self._fn_name, flow_input)
        if not self._is_activity:
            return StartSubtask(*activity_args, version=self.fn_version, lambda_role=kwargs.get('lambda_role'))
        return StartActivity(*activity_args, version=self.fn_version, **kwargs)

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

    @property
    def results(self):
        results = json.loads(self._results, cls=AlgDecoder)
        return {self._fn_name: results}


class Chain:
    def __init__(self, signatures):
        self._signatures = signatures

    def __call__(self, *args, **kwargs):
        chain_results = {}
        for signature in self._signatures:
            if not signature.is_started:
                signature(**kwargs)
                return
            if signature.is_failed:
                signature(**kwargs)
                return
            if not signature.is_complete and not signature.is_failed:
                return
            results = signature.results
            chain_results = results
            kwargs['task_args'].add_arguments(results)
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

    @property
    def results(self):
        results = {}
        for signature in self._signatures:
            results.update(signature.results)
        return results

    def __call__(self, *args, **kwargs):
        results = {}
        group_started = True
        progress = 0
        for signature in self._signatures:
            if not signature.is_started:
                signature(**kwargs)
                group_started = False
            progress += 1
            logging.debug(f'arbitrated a signature within a group call, progress: {progress}/{len(self._signatures)}')
        if not group_started:
            return
        for signature in self._signatures:
            if signature.is_failed:
                signature(**kwargs)
                return
            if not signature.is_complete and not signature.is_failed:
                return
            results.update(signature.results)
        return results


def chain(*args, **kwargs):
    return Chain(args)


def group(*args, **kwargs):
    return Group(args)
