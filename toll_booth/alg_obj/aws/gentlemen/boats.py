import json

from toll_booth.alg_obj.aws.gentlemen.decisions import StartActivity
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder


class ActivitySignature:
    def __init__(self, fn_name, fn_version, fn_identifier, fn_kwargs, is_started, is_complete, is_failed, fail_count, results):
        self._fn_name = fn_name
        self._fn_version = fn_version
        self._fn_identifier = fn_identifier
        self._fn_kwargs = fn_kwargs
        self._is_started = is_started
        self._is_complete = is_complete
        self._is_failed = is_failed
        self._fail_count = fail_count
        self._results = results

    @classmethod
    def from_work_history(cls, fn_identifier, fn_name, fn_kwargs=None, **kwargs):
        if not fn_kwargs:
            fn_kwargs = {}
        activities = kwargs['activities']
        versions = kwargs['versions']
        fn_version = versions.task_versions[fn_name]
        if fn_identifier not in activities.operation_ids:
            return cls(
                fn_name, fn_version, fn_identifier, fn_kwargs,
                is_started=False, is_complete=False,
                is_failed=False, fail_count=0, results=None
            )
        activity_operation = activities[fn_identifier]
        results = activity_operation.results
        # results = json.loads(results, cls=AlgDecoder)
        return cls(
            fn_name, fn_version, fn_identifier, fn_kwargs,
            is_started=True,
            is_complete=activity_operation.is_complete,
            is_failed=activity_operation.is_failed,
            fail_count=activities.get_operation_failed_count(fn_identifier),
            results=results
        )

    def __call__(self, *args, **kwargs):
        if self._is_complete:
            return self._results
        if self._is_failed:
            if self._fail_count > 3:
                raise RuntimeError('failure in activity signature for : %s' % self.fn_identifier)
        if self._is_started and not self._is_failed:
            return
        input_kwargs = kwargs['input_kwargs']
        decisions = kwargs['decisions']
        input_kwargs.update(self._fn_kwargs)
        input_kwargs['names'] = kwargs['names']
        flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
        start_activity = StartActivity(self._fn_identifier, self._fn_name, flow_input, version=self.fn_version, **kwargs)
        decisions.append(start_activity)

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
        return {self._fn_identifier: self._results}


class ActivityChain:
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
            kwargs['input_kwargs'].update(results)
        return chain_results


class ActivityGroup:
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
        for signature in self._signatures:
            if not signature.is_started:
                signature(**kwargs)
                group_started = False
        if not group_started:
            return
        for signature in self._signatures:
            if signature.is_failed:
                if signature.fail_count > 3:
                    raise RuntimeError('failed activity present in group')
                signature(**kwargs)
                return
            if not signature.is_complete and not signature.is_failed:
                return
            results.update(signature.results)
        return results


def chain(*args, **kwargs):
    return ActivityChain(args)


def group(*args, **kwargs):
    return ActivityGroup(args)
