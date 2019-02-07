import logging
import os

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph.index_manager.indexes import EmptyIndexException, UniqueIndex, \
    UniqueIndexViolationException, MissingIndexedPropertyException, AttemptedStubIndexException, Index
from toll_booth.alg_obj.graph.index_manager.undocumented_links import LinkHistory, LinkEntry


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
        indexes = kwargs.get('indexes', [])
        indexes.extend([object_index, internal_id_index, identifier_stem_index])
        self._table_name = table_name
        self._object_index = object_index
        self._internal_id_index = internal_id_index
        self._identifier_stem_index = identifier_stem_index
        self._link_index = link_index
        self._table = boto3.resource('dynamodb').Table(self._table_name)
        self._indexes = indexes
        self._other_indexes_key_name = other_indexes_key_name

    @classmethod
    def from_graph_schema(cls, graph_schema, **kwargs):
        indexes = []
        for vertex_entry in graph_schema.vertex_entries:
            for index_name, index_entry in vertex_entry.indexes.items():
                index_type = 'non_unique'
                if index_entry.is_unique:
                    index_type = 'unique'
                index = Index(index_name, index_entry.indexed_fields, index_type, vertex_entry.object_type)
                indexes.append(index)
        for edge_entry in graph_schema.edge_entries:
            for index_name, index_entry in edge_entry.indexes.items():
                index_type = 'non_unique'
                if index_entry.is_unique:
                    index_type = 'unique'
                index = Index(index_name, index_entry.indexed_fields, index_type, edge_entry.object_type)
                indexes.append(index)
        kwargs['indexes'] = indexes
        return cls(**kwargs)

    def query_object_max(self, identifier_stem):
        index_name = self._object_index.index_name
        query_args = {
            'Limit': 1,
            'ScanIndexForward': False,
            'KeyConditionExpression': Key('identifier_stem').eq(str(identifier_stem)),
            'TableName': self._table_name,
            'IndexName': index_name
        }
        results = self._table.query(**query_args)
        try:
            return int(results['Items'][0]['id_value'])
        except IndexError:
            raise EmptyIndexException(index_name)

    def get_links_iterator(self, linked_object_identifier_stem):
        paginator = boto3.client('dynamodb').get_paginator('query')
        query_args = {
            'TableName': self._table_name,
            'KeyConditionExpression': 'identifier_stem = :id',
            'ExpressionAttributeValues': {':id': {'S': str(linked_object_identifier_stem)}}
        }
        pages = paginator.paginate(**query_args)
        for page in pages:
            for item in page['Items']:
                yield LinkHistory.parse_from_table_entry(item)

    def _get_local_id_values(self, identifier_stem, index_name=None):
        paginator = boto3.client('dynamodb').get_paginator('query')
        if not index_name:
            index_name = self._object_index.index_name
        query_args = {
            'TableName': self._table_name,
            'IndexName': index_name,
            'KeyConditionExpression': 'identifier_stem = :id',
            'ExpressionAttributeValues': {':id': {'S': str(identifier_stem)}}
        }
        pages = paginator.paginate(**query_args)
        results = set()
        for page in pages:
            items = page['Items']
            total = len(items)
            progress = 0
            for item in items:
                id_value = item['id_value']['N']
                results.add(id_value)
                progress += 1
                logging.debug(f'{progress}/{total}')
        return results

    def get_local_id_values(self, identifier_stem, index_name=None, by_linked=None):
        local_id_values = self._get_local_id_values(identifier_stem, index_name)
        if not by_linked:
            return local_id_values
        links = self.get_links_iterator(identifier_stem)
        results = {'all': local_id_values, 'linked': set(), 'unlinked': set()}
        for link_history in links:
            id_value = link_history.id_value
            is_linked = link_history.currently_linked
            results_category = 'unlinked'
            if is_linked:
                results_category = 'linked'
            results[results_category].add(id_value)
        return results

    def index_object(self, graph_object):
        for index in self._indexes:
            if index.check_object_type(graph_object):
                missing_properties = index.check_for_missing_object_properties(graph_object)
                if missing_properties:
                    raise MissingIndexedPropertyException(index.index_name, index.indexed_fields, missing_properties)
                try:
                    self._index_object(index, graph_object)
                except AttemptedStubIndexException:
                    self._index_stub(graph_object)

    def add_link_event(self, link_utc_timestamp, potential_vertex, **kwargs):
        link_history = LinkHistory.for_first_link(potential_vertex, link_utc_timestamp)
        if 'put' in kwargs:
            self.index_object(potential_vertex)
            self.index_object(link_history)
            return
        if 'unlink' in kwargs:
            link_entry = LinkEntry(link_utc_timestamp, is_unlink=True)
            self._perform_link_operation(link_history, link_entry)
            return
        if 'link' in kwargs:
            link_entry = LinkEntry(link_utc_timestamp, is_unlink=False)
            self._perform_link_operation(link_history, link_entry)
            return
        raise NotImplementedError(f'could not find link operation for {kwargs}')

    @classmethod
    def _convert_graph_object(cls, graph_object):
        if not graph_object.is_identifiable:
            return None
        return graph_object.for_index

    def _index_object(self, index, graph_object):
        item = self._convert_graph_object(graph_object)
        if item is None:
            raise AttemptedStubIndexException(index.index_name, graph_object)
        args = {
            'Item': item
        }
        if index.is_unique:
            args['ConditionExpression'] = index.conditional_statement
        try:
            self._table.put_item(**args)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
            raise UniqueIndexViolationException(index.index_name, item)

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
        partition_key_name, hash_key_name = self._object_index.indexed_fields
        key_value = {
            partition_key_name: str(link_history.identifier_stem),
            hash_key_name: str(link_history.id_value)
        }
        self._table.update_item(
            Key=key_value,
            UpdateExpression="ADD link_entries :le",
            ExpressionAttributeValues={':le': link_entry.for_index}
        )
