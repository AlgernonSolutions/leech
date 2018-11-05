import os
import threading
from collections import deque
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr


class DynamoScanner:
    def __init__(self, index_name, **kwargs):
        table_name = kwargs.get('table_name', os.getenv('TABLE_NAME', 'GraphObjects'))
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)
        self._worker_count = kwargs.get('thread_count', 5)
        self._index_name = index_name
        self._workers = []
        self._results = deque()

    def scan_leech_stages(self):
        scan_kwargs = {'ProjectionExpression': 'last_stage_seen, sid_value'}
        self._scan(scan_kwargs, self._sort_stage_items)
        stages = {}
        for result in self._results:
            for stage_name, sid_values in result.items():
                if stage_name not in stages:
                    stages[stage_name] = []
                stages[stage_name].extend(sid_values)
        return stages

    def scan_stalled_objects(self):
        scan_kwargs = {
            'FilterExpression': Attr('last_seen_time').lt(self._get_expiration_timestamp()) & Attr('last_stage_seen').ne('graphing')
        }
        self._scan(scan_kwargs)
        results = []
        for result in self._results:
            results.extend(result)
        return results

    def _scan(self, scan_kwargs, sort_function=None):
        self._results = deque()
        for thread in range(self._worker_count):
            thread_kwargs = {'worker_number': thread, 'scan_kwargs': scan_kwargs, 'sorting': sort_function}
            t = threading.Thread(target=self._paginate_scan, kwargs=thread_kwargs)
            t.start()
            self._workers.append(t)
        for thread in self._workers:
            thread.join()

    def _paginate_scan(self, worker_number, scan_kwargs, sorting):
        results, token = self._scan_table(worker_number, scan_kwargs, sorting)
        self._results.append(results)
        while token:
            results, token = self._scan_table(worker_number, scan_kwargs, sorting, token)
            self._results.append(results)

    def _scan_table(self, worker_number, scan_kwargs, sort_function, token=None):
        scan_kwargs.update({
            'Segment': worker_number,
            'TotalSegments': self._worker_count,
            'TableName': self._table_name,
            'IndexName': self._index_name
        })
        if token:
            scan_kwargs['ExclusiveStartKey'] = token
        results = self._table.scan(**scan_kwargs)
        items = results['Items']
        if sort_function:
            items = sort_function(items)
        return items, results.get('LastEvaluatedKey', None)

    @classmethod
    def _sort_stage_items(cls, items):
        stages = {}
        for entry in items:
            stage_name = entry['last_stage_seen']
            sid_value = entry['sid_value']
            if stage_name not in stages:
                stages[stage_name] = []
            stages[stage_name].append(sid_value)
        return stages

    @classmethod
    def _get_expiration_timestamp(cls):
        import datetime
        expiration_timestamp = (datetime.datetime.now() - datetime.timedelta(minutes=30)).timestamp()
        return Decimal(expiration_timestamp)
