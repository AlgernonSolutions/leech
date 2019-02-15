import json
import logging
import os

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph.index_manager.indexes import EmptyIndexException, UniqueIndex, \
    UniqueIndexViolationException, MissingIndexedPropertyException, AttemptedStubIndexException, Index
from toll_booth.alg_obj.graph.index_manager.undocumented_links import LinkHistory, LinkEntry
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem, PotentialVertex
from toll_booth.alg_obj.graph.schemata.schema import Schema
from toll_booth.alg_obj.serializers import ExplosionDecoder, AlgDecoder


class IndexManager:
    def __init__(self, table_name=None, **kwargs):
        object_index = kwargs.get('object_index', None)
        internal_id_index = kwargs.get('internal_id_index', None)
        link_index = kwargs.get('link_index', None)
        identifier_stem_index = kwargs.get('identifier_stem_index', None)
        if table_name is None:
            table_name = os.getenv('INDEX_TABLE_NAME', 'leech_indexes')
        if object_index is None:
            object_index = UniqueIndex.for_object_index(**kwargs)
        if internal_id_index is None:
            internal_id_index = UniqueIndex.for_internal_id_index(**kwargs)
        if identifier_stem_index is None:
            identifier_stem_index = UniqueIndex.for_identifier_stem_index(**kwargs)
        if link_index is None:
            link_index = UniqueIndex.for_link_index(**kwargs)
        other_indexes_key_name = kwargs.get('index_key_name', os.getenv('INDEXES_KEY_NAME', 'indexes'))
        other_indexes = kwargs.get('indexes', [])
        indexes = [object_index, internal_id_index, identifier_stem_index]
        self._table_name = table_name
        self._object_index = object_index
        self._internal_id_index = internal_id_index
        self._identifier_stem_index = identifier_stem_index
        self._link_index = link_index
        self._table = boto3.resource('dynamodb').Table(self._table_name)
        self._indexes = indexes
        self._other_indexes = other_indexes
        self._other_indexes_key_name = other_indexes_key_name

    @classmethod
    def from_graph_schema(cls, graph_schema: Schema, **kwargs):
        indexes = []
        for vertex_entry in graph_schema.vertex_entries.values():
            for index_name, index_entry in vertex_entry.indexes.items():
                index_type = 'non_unique'
                if index_entry.is_unique:
                    index_type = 'unique'
                index = Index(index_name, index_entry.indexed_fields, index_type, vertex_entry.object_type)
                indexes.append(index)
        for edge_entry in graph_schema.edge_entries.values():
            for index_name, index_entry in edge_entry.indexes.items():
                index_type = 'non_unique'
                if index_entry.is_unique:
                    index_type = 'unique'
                index = Index(index_name, index_entry.indexed_fields, index_type, edge_entry.object_type)
                indexes.append(index)
        kwargs['indexes'] = indexes
        return cls(**kwargs)

    def query_object_max(self, identifier_stem):
        query_args = {
            'Limit': 1,
            'ScanIndexForward': False,
            'KeyConditionExpression': Key('identifier_stem').eq(str(identifier_stem)),
            'TableName': self._table_name,
            'IndexName': self._identifier_stem_index.index_name
        }
        results = self._table.query(**query_args)
        try:
            return results['Items'][0]['numeric_id_value']
        except IndexError:
            raise EmptyIndexException(self._object_index.index_name)

    def get_links_iterator(self, linked_object_identifier_stem):
        paginator = boto3.client('dynamodb').get_paginator('query')
        paired_identifiers = linked_object_identifier_stem.paired_identifiers
        link_identifier_stem = IdentifierStem('edge', '_fip_link_', paired_identifiers)
        query_args = {
            'TableName': self._table_name,
            'KeyConditionExpression': 'identifier_stem = :id',
            'ExpressionAttributeValues': {':id': {'S': str(link_identifier_stem)}}
        }
        pages = paginator.paginate(**query_args)
        for page in pages:
            items = json.loads(json.dumps(page['Items']), cls=ExplosionDecoder)
            for item in items:
                yield LinkHistory.parse_from_table_entry(item)

    def _get_link_histories(self, identifier_stem, index_name=None):
        paginator = boto3.client('dynamodb').get_paginator('query')
        query_args = {
            'TableName': self._table_name,
            'KeyConditionExpression': 'identifier_stem = :id',
            'ExpressionAttributeValues': {':id': {'S': str(identifier_stem)}}
        }
        if index_name:
            query_args['IndexName'] = index_name
        pages = paginator.paginate(**query_args)
        results = []
        for page in pages:
            items = json.loads(json.dumps(page['Items']), cls=ExplosionDecoder)
            total = len(items)
            progress = 0
            for item in items:
                link_history = LinkHistory.parse_from_table_entry(item)
                results.append(link_history)
                progress += 1
                logging.debug(f'{progress}/{total}')
        return results

    def get_local_id_values(self, identifier_stem, index_name=None):
        link_histories = self._get_link_histories(identifier_stem, index_name)
        local_id_values = set(x.id_value for x in link_histories)
        results = {'all': local_id_values, 'linked': set(), 'unlinked': set()}
        for link_history in link_histories:
            id_value = link_history.id_value
            is_linked = link_history.currently_linked
            results_category = 'unlinked'
            if is_linked:
                results_category = 'linked'
            results[results_category].add(id_value)
        return results

    def index_object(self, graph_object, link_values=None):
        item = self._convert_graph_object(graph_object, link_values)
        for index in self._indexes:
            if item is None:
                raise AttemptedStubIndexException(index.index_name, graph_object)
            if index.check_object_type(graph_object):
                missing_properties = index.check_for_missing_object_properties(graph_object)
                if missing_properties:
                    raise MissingIndexedPropertyException(index.index_name, index.indexed_fields, missing_properties)
        try:
            self._index_object(graph_object, link_values=link_values)
        except AttemptedStubIndexException:
            self._index_stub(graph_object)

    def add_link_events(self, link_utc_timestamp, potential_vertexes, **kwargs):
        event_results = []
        for vertex in potential_vertexes:
            event_result = self.add_link_event(link_utc_timestamp, vertex, **kwargs)
            event_results.append(event_result)
        return event_results

    def add_link_event(self, link_utc_timestamp, potential_vertex, **kwargs):
        link_history = LinkHistory.for_first_link(potential_vertex, link_utc_timestamp)
        if 'put' in kwargs:
            link_values = {x.for_index for x in link_history.link_entries}
            self.index_object(potential_vertex, link_values=link_values)
            return link_history
        if 'unlink' in kwargs:
            link_entry = LinkEntry(link_utc_timestamp, is_unlink=True)
            self._perform_link_operation(link_history, link_entry)
            return link_history, link_entry
        if 'link' in kwargs:
            link_entry = LinkEntry(link_utc_timestamp, is_unlink=False)
            self._perform_link_operation(link_history, link_entry)
            return link_history, link_entry
        raise NotImplementedError(f'could not find link operation for {kwargs}')

    def find_potential_vertexes(self, object_type, vertex_properties):
        potential_vertexes, token = self._scan_vertexes(object_type, vertex_properties)
        while token:
            more_vertexes, token = self._scan_vertexes(object_type, vertex_properties, token)
            potential_vertexes.extend(more_vertexes)
        logging.info('completed a scan of the data space to find potential vertexes with properties: %s '
                     'returned the raw values of: %s' % (vertex_properties, potential_vertexes))
        return [PotentialVertex.from_json(x) for x in potential_vertexes]

    @classmethod
    def _convert_graph_object(cls, graph_object, link_values=None):
        if not graph_object.is_identifiable:
            return None
        index_object = graph_object.for_index
        if link_values:
            index_object['links'] = link_values
        return index_object

    def _index_object(self, graph_object, link_values=None):
        item = self._convert_graph_object(graph_object, link_values)
        args = {
            'Item': item
        }
        condition_expressions = set()
        unique_index_names = []
        for index in self._indexes:
            if index.is_unique:
                condition_expressions.update(index.conditional_statement)
                unique_index_names.append(index.index_name)
        if condition_expressions:
            args['ConditionExpression'] = ' AND '.join(condition_expressions)
        try:
            self._table.put_item(**args)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
            raise UniqueIndexViolationException(', '.join(unique_index_names), item)

    def _index_stub(self, stub_graph_object):
        object_keys = self._object_index.indexed_fields
        stub_key = {
            object_keys[0]: 'stub',
            object_keys[1]: stub_graph_object.object_type
        }
        args = {
            'Key': stub_key,
            'UpdateExpression': 'ADD stub_value :sv',
            'ExpressionAttributeValues': {':sv': set(stub_graph_object.for_stub_index)}
        }
        try:
            self._table.update_item(**args)
        except ClientError as e:
            raise e

    def _perform_link_operation(self, link_history: LinkHistory, link_entry: LinkEntry):
        hash_key_name, partition_key_name = self._object_index.indexed_fields
        key_value = {
            partition_key_name: str(link_history.identifier_stem),
            hash_key_name: str(link_history.id_value)
        }
        self._table.update_item(
            Key=key_value,
            UpdateExpression="ADD link_entries :le",
            ExpressionAttributeValues={':le': {link_entry.for_index}}
        )

    def _scan_vertexes(self, object_type, vertex_properties, token=None):
        filter_properties = [f'(begins_with(identifier_stem, :is) OR begins_with(identifier_stem, :stub))']
        expression_names = {}
        expression_values = {
            ':is': f'#vertex#{object_type}#',
            ':stub': '#vertex#stub#',
        }
        pointer = 1
        for property_name, vertex_property in vertex_properties.items():
            if hasattr(vertex_property, 'is_missing'):
                continue
            filter_properties.append(f'object_properties.#{pointer} = :property{pointer}')
            expression_names[f'#{pointer}'] = property_name
            expression_values[f':property{pointer}'] = vertex_property
            pointer += 1
        scan_kwargs = {
            'FilterExpression': ' AND '.join(filter_properties),
            'ExpressionAttributeNames': expression_names,
            'ExpressionAttributeValues': expression_values
        }
        if token:
            scan_kwargs['ExclusiveStartKey'] = token
        results = self._table.scan(**scan_kwargs)
        return results['Items'], results.get('LastEvaluatedKey', None)
