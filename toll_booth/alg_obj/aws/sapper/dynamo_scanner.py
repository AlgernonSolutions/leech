import os
import threading
from collections import deque

import boto3


class LeechScanner:
    _internal_id_index = os.getenv('INTERNAL_ID_INDEX', 'internal_ids')
    _id_value_index = os.getenv('ID_VALUE_INDEX', 'id_values')

    def __init__(self, **kwargs):
        table_name = kwargs.get('table_name', os.getenv('TABLE_NAME', 'GraphObjects'))
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)
        self._worker_count = kwargs.get('thread_count', 5)
        self._stage_index = kwargs.get('stage_index', 'stages')
        self._workers = []
        self._results = deque()

    def scan(self):
        for thread in range(self._worker_count):
            t = threading.Thread(target=self._scan_leech_stages, kwargs={'worker_number': thread})
            t.start()
            self._workers.append(t)
        for thread in self._workers:
            thread.join()
        stages = {}
        for result in self._results:
            stage_name = result['stage_name']
            if stage_name not in stages:
                stages[stage_name] = []
            stages[stage_name].extend(result['sid_values'])
        return stages

    def _scan_leech_stages(self, worker_number):
        results, token = self.__scan(worker_number)
        self._queue_results(results)
        while token:
            results, token = self.__scan(worker_number, token)
            self._queue_results(results)

    def _queue_results(self, results):
        for stage_name, sid_values in results.items():
            self._results.append({'stage_name': stage_name, 'sid_values': sid_values})

    def __scan(self, worker_number, token=None):
        scan_kwargs = {
            'TableName': self._table_name,
            'IndexName': self._stage_index,
            'ProjectionExpression': 'last_stage_seen, sid_value',
            'Segment': worker_number,
            'TotalSegments': self._worker_count
        }
        if token:
            scan_kwargs['ExclusiveStartKey'] = token
        results = self._table.scan(**scan_kwargs)
        items = results['Items']
        sorted_items = self._sort_items(items)
        return sorted_items, results.get('LastEvaluatedKey', None)

    @classmethod
    def _sort_items(cls, items):
        stages = {}
        for entry in items:
            stage_name = entry['last_stage_seen']
            sid_value = entry['sid_value']
            if stage_name not in stages:
                stages[stage_name] = []
            stages[stage_name].append(sid_value)
        return stages
