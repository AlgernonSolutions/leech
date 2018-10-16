import json
import os

import boto3

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.aws.aws_obj.lockbox import IndexDriver, IndexKey
from toll_booth.alg_obj.aws.trident.graph_driver import TridentDriver, TridentScanner
from toll_booth.alg_obj.graph.ogm.generator import CommandGenerator
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEntry
from toll_booth.alg_obj.serializers import AlgEncoder


class AuditQueue:
    def __init__(self, **kwargs):
        queue_url = kwargs.get('audit_url', os.getenv('AUDIT_URL', None))
        sqs = boto3.resource('sqs')
        self._queue = sqs.Queue(queue_url)

    def send_audit_task(self, audit_target, assigned_vertexes, rebuild):
        task_args = {
            'audit_target': audit_target,
            'assigned_vertexes': assigned_vertexes,
            'rebuild': rebuild
        }
        message_body = {
            'task_name': 'audit',
            'task_args': json.dumps(task_args, cls=AlgEncoder)
        }
        self._queue.send_message(
            MessageBody=json.dumps(message_body, cls=AlgEncoder)
        )


class AuditResultsQueue:
    def __init__(self, **kwargs):
        queue_url = kwargs.get('audit_results_url', os.getenv('AUDIT_RESULTS_URL', None))
        sqs = boto3.resource('sqs')
        self._queue = sqs.Queue(queue_url)

    def send_audit_results(self, missing_internal_ids):
        for missing_id in missing_internal_ids:
            self._queue.send_message(missing_id)


class AuditTarget(AlgObject):
    def __init__(self, object_type, object_properties=None, is_edge=False):
        if not object_properties:
            object_properties = {}
        self._object_type = object_type
        self._object_properties = object_properties
        self._is_edge = is_edge

    @property
    def to_json(self):
        return {
            'object_type': self._object_type,
            'object_properties': self._object_properties,
            'is_edge': self._is_edge
        }

    @property
    def object_type(self):
        return self._object_type

    @property
    def object_properties(self):
        return self._object_properties

    @property
    def for_index_key(self):
        properties = {
            'object_type': self._object_properties,
            'is_edge': self._is_edge,
        }
        properties.update(self.object_properties)
        return properties

    @property
    def is_edge(self):
        return self._is_edge

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['object_type'], json_dict.get('object_properties', None), json_dict.get('is_edge', False)
        )


class ScanAuditor:
    def __init__(self, audit_target, **kwargs):
        self._queue = AuditQueue()
        self._audit_target = audit_target
        segment_size = kwargs.get('segment_size', 50)
        self._trident_scanner = kwargs.get('trident_driver',
                                           TridentScanner(segment_size=segment_size, **audit_target.to_json))
        self._segment_size = segment_size

    def segment_graph(self, rebuild=False):
        cursor = 0
        cursor, results = self._trident_scanner.scan(cursor)
        self._queue.send_audit_task(self._audit_target, results, rebuild)
        while cursor != 0:
            cursor, results = self._trident_scanner.scan(cursor)
            self._queue.send_audit_task(self._audit_target, results, rebuild)


class ScanAuditWorker:
    def __init__(self, audit_target, assigned_vertexes, **kwargs):
        self._trident_driver = TridentDriver(**kwargs)
        self._index_driver = IndexDriver(**kwargs)
        self._schema_entry = SchemaEntry.get(audit_target.object_type)
        self._rebuild = kwargs.get('rebuild', False)
        self._audit_target = audit_target
        self._assigned_vertexes = assigned_vertexes
        self._assigned_vertexes.sort(key=lambda x: x.vertex_id, reverse=False)

    @property
    def min_vertex(self):
        return self._assigned_vertexes[0]

    @property
    def max_vertex(self):
        return self._assigned_vertexes[len(self._assigned_vertexes) - 1]

    @property
    def vertex_internal_ids(self):
        return set(x.vertex_id for x in self._assigned_vertexes)

    def clear_indexes(self):
        self._index_driver.client.flushall()

    def _vertex_property_values(self, property_name):
        if property_name == 'internal_id':
            return self.vertex_internal_ids
        return set(x.vertex_properties[property_name][0].value for x in self._assigned_vertexes)

    def scan(self):
        if self._rebuild:
            return self.rebuild()
        scan_results = set()
        for index_name, index_entry in self._schema_entry.indexes.items():
            missing = self._evaluate_index_sample(index_entry)
            scan_results.update(missing)
        return scan_results

    def rebuild(self):
        generator = CommandGenerator(self._schema_entry)
        with self._index_driver.client.pipeline() as pipe:
            for vertex in self._assigned_vertexes:
                object_properties = {
                    x: y[0].value for x, y in vertex.vertex_properties.items()
                }
                potential_vertex = PotentialVertex(vertex.vertex_label, vertex.vertex_id, object_properties, False)
                commands = generator.create_index_graph_object_commands(potential_vertex)
                for command in commands:
                    pipe.execute_command(*command)

    def _evaluate_index_sample(self, index_entry):
        if index_entry.index_type == 'unique':
            return self._evaluate_unique_index(index_entry)
        if index_entry.index_type == 'sorted_set':
            index_key = IndexKey.from_object_properties(self._audit_target.for_index_key, index_entry)
            results = self._evaluate_sorted_set_index(index_key, index_entry)
            return results

    def _evaluate_sorted_set_index(self, index_key, index_entry):
        is_lex = self._is_sorted_set_lex(index_key)
        if is_lex:
            return self._evaluate_lex_sample(index_key)
        return self._evaluate_int_sample(index_key, index_entry)

    def _evaluate_lex_sample(self, index_key):
        results = self._index_driver.client.zrangebylex(
            index_key, f'[{self.min_vertex.vertex_id}', f'[{self.max_vertex.vertex_id}')
        return self.vertex_internal_ids - set(x.decode() for x in results)

    def _evaluate_int_sample(self, index_key, index_entry):
        missing = set()
        score_field_name = index_entry.score_field
        vertex_score_values = [x.vertex_properties[score_field_name][0].value for x in self._assigned_vertexes]
        with self._index_driver.client.pipeline() as pipe:
            for value in vertex_score_values:
                pipe.zrangebyscore(index_key, value, value, withscores=True)
            results = pipe.execute()
            indexed = {int(x[0][1]): x[0][0].decode() for x in results}
            if self.vertex_internal_ids - set(indexed.values()):
                missing.update(self.vertex_internal_ids - set(indexed.keys()))
            if self._vertex_property_values(score_field_name) - set(indexed.keys()):
                missing.update(
                    [indexed[x] for x in (self._vertex_property_values(score_field_name) - set(indexed.keys()))])
            return missing

    def _evaluate_unique_index(self, index_entry):
        missing = []
        with self._index_driver.client.pipeline() as pipe:
            for vertex in self._assigned_vertexes:
                index_key = IndexKey.from_object_properties(vertex.object_properties, index_entry)
                pipe.get(index_key)
            results = pipe.execute()
            indexed_internal_ids = set(x.decode() for x in results)
            for internal_id in (self.vertex_internal_ids - indexed_internal_ids):
                missing.append(internal_id)
        return missing

    def _is_sorted_set_lex(self, index_key):
        test_results = self._index_driver.client.zrange(index_key, '0', '0', withscores=True)
        return test_results[0][1] == 0.0
