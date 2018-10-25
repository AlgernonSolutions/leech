import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex, PotentialEdge


class DynamoDriver:
    _partition_key = os.getenv('PARTITION_KEY', 'identifier_stem')
    _sort_key = os.getenv('SORT_KEY', 'sid_value')
    _stub_key = {_partition_key: 'stub', _sort_key: '0'}
    _internal_id_index = os.getenv('INTERNAL_ID_INDEX', 'internal_ids')
    _id_value_index = os.getenv('ID_VALUE_INDEX', 'id_values')
    _no_overwrite_condition = Attr(_partition_key).not_exists() & Attr(_sort_key).not_exists()

    def __init__(self, table_name=None):
        if not table_name:
            table_name = os.getenv('TABLE_NAME', 'GraphObjects')
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)

    def query_index_value_max(self, identifier_stem, index_name=None):
        if not index_name:
            index_name = self._id_value_index
        query_args = {
            'Limit': 1,
            'ScanIndexForward': False,
            'KeyConditionExpression': Key('identifier_stem').eq(identifier_stem),
            'TableName': self._table_name,
            'IndexName': index_name
        }
        results = self._table.query(**query_args)
        try:
            max_id = results['Items'][0]['id_value']
        except IndexError:
            max_id = 0
        return max_id

    def find_potential_vertexes(self, vertex_properties):
        potential_vertexes, token = self._scan_vertexes(vertex_properties)
        while token:
            more_vertexes, token = self._scan_vertexes(vertex_properties, token)
            potential_vertexes.extend(more_vertexes)
        return [PotentialVertex.from_json(x) for x in potential_vertexes]

    def _scan_vertexes(self, vertex_properties, token=None):
        filter_properties = []
        expression_names = {}
        expression_values = {}
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

    def put_vertex_seed(self, object_type, identifier_stem, id_value, stage_name):
        now = self._get_decimal_timestamp()
        return self._table.put_item(
            Item={
                self._partition_key: identifier_stem,
                self._sort_key: str(id_value),
                'id_value': id_value,
                'object_type': object_type,
                'is_edge': False,
                'completed': False,
                'disposition': 'working',
                'last_stage_seen': stage_name,
                f'{stage_name}_clear_time': now,
                'last_seen_time': now
            },
            ConditionExpression=self._no_overwrite_condition
        )

    def get_vertex(self, identifier_stem, id_value):
        results = self._table.get_item(
            Key={self._partition_key: identifier_stem, self._sort_key: str(id_value)}
        )
        try:
            vertex_information = results['Item']
        except KeyError:
            return None
        return PotentialVertex.from_json(vertex_information)

    def write_vertex(self, vertex, stage_name):
        if not vertex.is_identifiable:
            raise RuntimeError(
                f'could not uniquely identify a ruled vertex for type: {vertex.object_type}')
        if not vertex.is_properties_complete:
            raise RuntimeError(
                f'could not derive all properties for ruled vertex type: {vertex.object_type}')
        update_expression = '''
            SET #i=:i, #o=:v, #d=:d, #lst=:t, #sc=:t, #ot=:ot, #im=:im, #c=:c, #lss=:lss, #idf=:idf, #id=:id
        '''
        return self._table.update_item(
            Key={self._partition_key: vertex.identifier_stem, self._sort_key: str(vertex.id_value)},
            UpdateExpression=update_expression,
            ExpressionAttributeValues={
                ':ot': vertex.object_type,
                ':v': vertex.object_properties,
                ':i': vertex.internal_id,
                ':im': vertex.if_missing,
                ':d': 'graphing',
                ':c': False,
                ':t': self._get_decimal_timestamp(),
                ':lss': stage_name,
                ':idf': vertex.id_value_field,
                ':id': vertex.id_value
            },
            ExpressionAttributeNames={
                '#i': 'internal_id',
                '#o': 'object_properties',
                '#d': 'disposition',
                '#lst': 'last_seen_time',
                '#sc': f'{stage_name}_clear_time',
                '#lss': 'last_stage_seen',
                '#ot': 'object_type',
                '#im': 'if_missing',
                '#c': 'completed',
                '#idf': 'id_value_field',
                '#id': 'id_value'
            },
            ConditionExpression=self._no_overwrite_condition
        )

    def write_edge(self, edge, stage_name):
        now = self._get_decimal_timestamp()
        edge_entry = {
            'object_type': edge.edge_label,
            self._partition_key: edge.identifier_stem,
            self._sort_key: edge.internal_id,
            'internal_id': edge.internal_id,
            'from_object': edge.from_object,
            'to_object': edge.to_object,
            'object_properties': edge.object_properties,
            'disposition': 'graphing',
            'completed': False,
            'last_stage_seen': stage_name,
            f'{stage_name}_clear_time': now,
            'last_seen_time': now
        }
        for property_name, object_property in edge.object_properties.items():
            edge_entry[property_name] = object_property
        try:
            return self._table.put_item(
                Item=edge_entry,
                ConditionExpression=self._no_overwrite_condition
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e

    def get_edge(self, edge_label, edge_internal_id):
        results = self._table.get_item(Key={self._partition_key: edge_label, self._sort_key: edge_internal_id})
        return PotentialEdge.from_json(results['Item'])

    def mark_ids_as_working(self, identifier_stem, id_values, object_type, stage_name='monitoring'):
        already_working = []
        not_working = []
        for id_value in id_values:
            try:
                self.put_vertex_seed(object_type, identifier_stem, id_value, stage_name)
                not_working.append(id_value)
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise e
                already_working.append(id_value)
        return already_working, not_working

    def mark_object_as_blank(self, identifier_stem, id_value):
        return self._table.update_item(
            Key={self._partition_key: identifier_stem, self._sort_key: str(id_value)},
            UpdateExpression='SET completed = :c, disposition = :d, last_stage_seen = :s, last_seen_time = :t',
            ExpressionAttributeValues={
                ':c': True,
                ':d': 'blank',
                ':s': 'extraction',
                ':t': self._get_decimal_timestamp()
            }
        )

    def mark_object_as_stage_cleared(self, identifier_stem, id_value, stage_name):
        return self._table.update_item(
            Key={self._partition_key: identifier_stem, self._sort_key: str(id_value)},
            UpdateExpression='SET last_stage_seen = :s, last_seen_time = :t, #sc = :t',
            ExpressionAttributeValues={
                ':s': stage_name,
                ':t': self._get_decimal_timestamp()
            },
            ExpressionAttributeNames={
                '#sc': f'{stage_name}_clear_time'
            },
            ConditionExpression=Attr(f'{stage_name}_clear_time').not_exists()
        )

    def add_stub_vertex(self, object_type, stub_properties, source_internal_id, rule_name):
        try:
            return self._add_stub_vertex(object_type, stub_properties, source_internal_id, rule_name)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ValidationException':
                raise e
            self._add_stub_object_type(object_type, stub_properties, source_internal_id, rule_name)

    def add_object_properties(self, identifier_stem, id_value, object_properties):
        update_components = self._generate_update_property_components(object_properties)
        return self._table.update_item(
            Key={self._partition_key: identifier_stem, self._sort_key: str(id_value)},
            UpdateExpression=update_components[2],
            ExpressionAttributeValues=update_components[1],
            ExpressionAttributeNames=update_components[0],
            ConditionExpression=Attr('vertex_properties').not_exists() & Attr('object_properties').not_exists()
        )

    def _add_stub_vertex(self, object_type, stub_properties, source_internal_id, rule_name):
        stub = {
            'object_properties': stub_properties,
            'source_internal_id': source_internal_id,
            'rule_name': rule_name
        }
        return self._table.update_item(
            Key=self._stub_key,
            UpdateExpression='SET #ot = list_append(#ot, :s)',
            ExpressionAttributeValues={
                ':s': [stub]
            },
            ExpressionAttributeNames={
                '#ot': object_type
            }
        )

    def _add_stub_object_type(self, object_type, stub_properties, source_internal_id, rule_name):
        stub = {
            'object_properties': stub_properties,
            'source_internal_id': source_internal_id,
            'rule_name': rule_name
        }
        return self._table.update_item(
            Key=self._stub_key,
            UpdateExpression='SET #ot = :s',
            ExpressionAttributeValues={
                ':s': [stub]
            },
            ExpressionAttributeNames={
                '#ot': object_type
            },
            ConditionExpression=Attr(object_type).not_exists()
        )

    @classmethod
    def _get_decimal_timestamp(cls):
        import datetime
        return Decimal(datetime.datetime.now().timestamp())

    @classmethod
    def _generate_update_property_components(cls, object_properties):
        update_parts = ['#v = :v', '#o = :v']
        expression_values = {
            ':v': object_properties
        }
        expression_names = {
            '#v': 'vertex_properties',
            '#o': 'object_properties'
        }
        counter = 0
        for property_name, vertex_property in object_properties.items():
            expression_name = f'#vp{counter}'
            expression_value = f':vp{counter}'
            expression_values[expression_value] = vertex_property
            expression_names[expression_name] = property_name
            update_parts.append(f'{expression_name} = {expression_value}')
            counter += 1
        update_expression = f'SET {", ".join(update_parts)}'
        return expression_names, expression_values, update_expression
