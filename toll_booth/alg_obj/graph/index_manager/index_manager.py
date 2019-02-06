import os

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph.index_manager.indexes import EmptyIndexException, UniqueIndex, \
    UniqueIndexViolationException, MissingIndexedPropertyException, AttemptedStubIndexException, Index


class IndexManager:
    def __init__(self, table_name=None, object_index=None, internal_id_index=None, **kwargs):
        if table_name is None:
            table_name = os.getenv('INDEX_TABLE_NAME', 'leech_indexes')
        if object_index is None:
            object_index = UniqueIndex.for_object_index(**kwargs)
        if internal_id_index is None:
            internal_id_index = UniqueIndex.for_internal_id_index(**kwargs)
        other_indexes_key_name = kwargs.get('index_key_name', os.getenv('INDEXES_KEY_NAME', 'indexes'))
        indexes = kwargs.get('indexes', [])
        indexes.extend([object_index, internal_id_index])
        self._table_name = table_name
        self._object_index = object_index
        self._internal_id_index = internal_id_index
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
