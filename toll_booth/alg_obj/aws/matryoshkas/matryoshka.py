import json
import logging
import threading
from collections import deque
from queue import Queue

import boto3
from botocore.config import Config

from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder
from toll_booth.alg_obj import AlgObject


class Matryoshka(AlgObject):
    def __init__(self, m_plan, m_concurrency, task_name, lambda_arn, task_constants, worker_args):
        self._m_plan = m_plan
        self._m_concurrency = m_concurrency
        self._task_constants = task_constants
        self._worker_args = worker_args
        self._task_name = task_name
        self._lambda_arn = lambda_arn
        self._work = Queue()
        self._results = deque()
        self._task_args = {}

        try:
            for plan_id, plan_entry in self._m_plan.items():
                plan = {'plan_id': plan_id, 'm_plan': plan_entry}
                self._work.put(plan)
            logging.info('this matryoshka is a branch, going to set up more workers')
            self._completed_results = self._unpack()
        except AttributeError:
            task_args = self._m_plan
            if not task_args:
                task_args = [{}]
            self._task_args = task_args
            logging.info(
                'this matryoshka is a worker, it is going to perform the task given it: %s:%s' % (task_name, task_args))
            self._completed_results = self._spin()

    @classmethod
    def parse_json(cls, json_dict):
        return cls.from_json(json_dict)

    @classmethod
    def from_json(cls, json_dict):
        try:
            task_args = json_dict['task_args']
            return cls(task_args['m_plan'], task_args['m_concurrency'], task_args['task_name'],
                       task_args['lambda_arn'], json_dict['task_constants'],
                       json_dict['worker_args'])
        except KeyError:
            task_args = json_dict['_task_args']
            return cls(task_args['_m_plan'], task_args['_m_concurrency'], task_args['_task_name'],
                       task_args['_lambda_arn'], json_dict['_task_constants'],
                       json_dict['_worker_args'])

    @classmethod
    def for_root(cls, matryoshka_cluster):
        return cls(*matryoshka_cluster.seed_args)

    def _unpack(self):
        thread_count = self._m_concurrency
        threads = []
        for worker in range(thread_count):
            t = threading.Thread(target=self.__unpack)
            t.start()
            threads.append(t)
        self._work.join()
        for _ in range(thread_count):
            self._work.put(None)
        for t in threads:
            t.join()
        return {x['child_position']: x['result'] for x in self._results}

    def __unpack(self):
        session = boto3.session.Session()
        client = session.client('lambda', config=Config(
            connect_timeout=315, read_timeout=315, max_pool_connections=25, retries={'max_attempts': 2}))
        function_arn = self._lambda_arn
        while True:
            plan = self._work.get()
            if plan is None:
                return
            plan_id = plan['plan_id']
            m_plan = plan['m_plan']
            payload = {
                'single': 1,
                'task_name': 'unpack_matryoshka',
                'task_args': {
                    'm_plan': m_plan,
                    'm_concurrency': self._m_concurrency,
                    'task_name': self._task_name,
                    'lambda_arn': self._lambda_arn
                },
                'task_constants': self._task_constants,
                'worker_args': self._worker_args
            }
            logging.info('firing lambda request while unpacking')
            response = client.invoke(
                FunctionName=function_arn,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload, cls=AlgEncoder)
            )
            raw_results = json.loads(response['Payload'].read())
            if 'errorMessage' in raw_results:
                if 'stackTrace' in raw_results:
                    stack_trace = raw_results['stackTrace']
                    logging.warning(stack_trace)
                    result = (raw_results['errorType'] + '  ' + raw_results['errorMessage'] + ' ' + '%s' % payload)
                else:
                    result = raw_results['errorMessage']
            else:
                result = json.loads(raw_results, cls=AlgDecoder)
            self._results.append({'child_position': plan_id, 'result': result})
            self._work.task_done()

    def _spin(self):
        task_args = self._task_args
        results = []
        for task_arg in task_args:
            task_name = self._task_name
            task_constants = self._task_constants
            worker_args = self._worker_args
            function_arn = self._lambda_arn
            task_arg.update(task_constants)
            session = boto3.session.Session()
            client = session.client('lambda', config=Config(
                connect_timeout=315, read_timeout=315, max_pool_connections=25, retries={'max_attempts': 2}))
            event = {
                'single': 1,
                'task_name': task_name,
                'task_args': task_arg,
                'worker_args': worker_args}
            response = client.invoke(
                FunctionName=function_arn,
                InvocationType='RequestResponse',
                Payload=json.dumps(event, cls=AlgEncoder)
            )
            raw_results = json.loads(response['Payload'].read())
            if 'errorMessage' in raw_results:
                stack_trace = raw_results['stackTrace']
                logging.warning(stack_trace)
                result = (raw_results['errorType'] + '  ' + raw_results['errorMessage'] + ' ' + '%s' % event)
            else:
                result = json.loads(raw_results, cls=AlgDecoder)
            results.append(result)
        return results

    @property
    def completed_results(self):
        return self._completed_results

    @property
    def aggregate_results(self):
        # noinspection PyTypeChecker
        return self._unpack_results(self.completed_results)

    def _unpack_results(self, result_dict):
        results = []
        for identifier, entry in result_dict.items():
            if isinstance(entry, dict):
                entry = self._unpack_results(entry)
            if isinstance(entry, list):
                results.extend(entry)
            else:
                results.append(entry)
        return results


class MatryoshkaCluster:
    def __init__(self, m_plan, task_name, lambda_arn, max_m_concurrency, task_constants=None, worker_args=None):
        if not task_constants:
            task_constants = {}
        if not worker_args:
            worker_args = {}
        self._m_plan = m_plan
        self._task_name = task_name
        self._lambda_arn = lambda_arn
        self._max_m_concurrency = max_m_concurrency
        self._task_constants = task_constants
        self._worker_args = worker_args

    @classmethod
    def calculate_for_concurrency(cls, desired_concurrency, task_name, lambda_arn, **kwargs):
        import math

        task_args = kwargs.get('task_args', {})
        task_constants = kwargs.get('task_constants', {})
        worker_args = kwargs.get('worker_args', {})
        max_m_concurrency = kwargs.get('max_m_concurrency', 20)
        num_branch_levels = int(math.floor((math.log(desired_concurrency) / math.log(max_m_concurrency))))
        root = MatryoshkaBranch()
        while root.current_concurrency < desired_concurrency:
            root.add_branch(num_branch_levels, max_m_concurrency)
        for task_arg in task_args:
            root.distribute_work(task_arg)
        if task_args:
            while root.current_concurrency > len(task_args):
                root.prune()
        return cls(root.children, task_name, lambda_arn, max_m_concurrency, task_constants, worker_args)

    @property
    def max_concurrency(self):
        return self._max_m_concurrency

    @property
    def seed_args(self):
        seed_args = (
            self._m_plan, self._max_m_concurrency, self._task_name,
            self._lambda_arn, self._task_constants, self._worker_args
        )
        return seed_args


class MatryoshkaBranch:
    def __init__(self, parent_branch_level=None, children=None, task_args=None):
        if children and task_args:
            raise NotImplementedError('current implementation only allows terminal branches to host flowers')
        self._parent_branch_level = parent_branch_level
        self._children = {}
        if not children:
            children = {}
        if not task_args:
            task_args = []
        self._children = children
        self._task_args = task_args

    def __len__(self):
        return self.current_concurrency

    def __gt__(self, other):
        if len(self) > len(other):
            return True
        return False

    def __lt__(self, other):
        if len(self) < len(other):
            return True
        return False

    @property
    def parent_branch_level(self):
        return self._parent_branch_level

    @property
    def branch_level(self):
        if self.parent_branch_level is None:
            return 0
        return self._parent_branch_level + 1

    @property
    def is_working_branch(self):
        if self.num_children > 0:
            return False
        return True

    @property
    def num_children(self):
        return len(self._children)

    @property
    def num_task_args(self):
        return len(self._task_args)

    @property
    def current_concurrency(self):
        workload = 0
        for child_id, child in self._children.items():
            if not child.is_working_branch:
                workload += child.current_concurrency
            else:
                workload += 1
        return workload

    @property
    def children(self):
        children = {}
        for child_id, child in self._children.items():
            children[child_id] = child.children
        if not children:
            return self._task_args
        return children

    @property
    def current_task_load(self):
        workload = 0
        for child_id, child in self._children.items():
            if not child.is_working_branch:
                workload += child.current_task_load
            else:
                workload += child.num_task_args
        if self.is_working_branch:
            return self.num_task_args
        return workload

    def add_branch(self, branch_levels, max_branches_per_level):
        if self.branch_level > branch_levels:
            return False
        if self.num_children >= max_branches_per_level:
            least_concurrent_child_id = min(self._children, key=self._children.get)
            success = self._children[least_concurrent_child_id].add_branch(branch_levels, max_branches_per_level)
            return success
        child_id = len(self._children)
        self._children[child_id] = MatryoshkaBranch(self.branch_level)
        return True

    def distribute_work(self, work_args):
        task_load = {}
        if self.is_working_branch:
            self._task_args.append(work_args)
            return True
        for child_id, child in self._children.items():
            task_load[child_id] = child.current_task_load
        least_tasked_child_id = min(task_load, key=task_load.get)
        success = self._children[least_tasked_child_id].distribute_work(work_args)
        return success

    def prune(self):
        empty_branches = []
        for child_id, child in self._children.items():
            if child.prune():
                empty_branches.append(child_id)
        if self.is_working_branch and not self.num_task_args:
            return True
        for empty_id in empty_branches:
            self._children.pop(empty_id, None)
