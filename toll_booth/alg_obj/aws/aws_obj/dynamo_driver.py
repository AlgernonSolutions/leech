import datetime
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex


class DynamoDriver:
    _stub_key = {'identifier_stem': 'stub', 'id_value': 0}

    def __init__(self, table_name=None):
        if not table_name:
            table_name = 'Seeds'
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)

    def query_index_max(self, identifier_stem):
        results = self._table.query(
            Limit=1,
            ScanIndexForward=False,
            KeyConditionExpression=Key('identifier_stem').eq(identifier_stem)
        )
        try:
            max_id = int(results['Items'][0]['id_value'])
        except IndexError:
            max_id = 0
        return max_id

    def find_potential_vertexes(self, vertex_properties):
        formatted_vertexes = []
        potential_vertexes, token = self._scan_vertexes(vertex_properties)
        while token:
            more_vertexes, token = self._scan_vertexes(vertex_properties, token)
            potential_vertexes.extend(more_vertexes)
        for potential_vertex in potential_vertexes:
            object_properties = {}
            for property_name, property_value in potential_vertex['object_properties'].items():
                if isinstance(property_value, Decimal):
                    property_value = int(property_value)
                object_properties[property_name] = property_value
            formatted_vertexes.append(
                PotentialVertex(
                    potential_vertex['object_type'], potential_vertex['internal_id'],
                    object_properties, potential_vertex.get('is_stub', False))
            )
        return formatted_vertexes

    def _scan_index_range(self, token, object_type, id_source, id_type, index_name=None):
        if not index_name:
            index_name = 'object_type-id_value-index'
        results = self._table.query(
            IndexName=index_name,
            TableName=self._table_name,
            Limit=1,
            Select='ALL_PROJECTED_ATTRIBUTES',
            ScanIndexForward=False,
            KeyConditionExpression=Key('object_type').eq(object_type),
            FilterExpression=Attr('id_source').eq(id_source) & Attr('id_type').eq(id_type),
            ExclusiveStartKey=token
        )
        try:
            max_id = results['Items'][0]['id_value']
        except IndexError:
            max_id = None
        return max_id, results.get('LastEvaluatedKey', None)

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

    def write_vertex(self, vertex):
        return self._table.update_item(
            Key={'identifier_stem': vertex.identifier_stem, 'id_value': vertex.id_value},
            UpdateExpression='SET #i = :i, #o = :v, #c = :c, #d = :d, #ls = :t, #sc = :t',
            ExpressionAttributeValues={
                ':v': vertex.object_properties,
                ':i': vertex.internal_id,
                ':d': 'graphing',
                ':t': datetime.datetime.now().timestamp()
            },
            ExpressionAttributeNames={
                '#i': 'internal_id',
                '#o': 'object_properties',
                '#d': 'disposition',
                '#ls': 'last_seen_time',
                '#sc': 'transform_clear_time'
            },
            ConditionExpression=Attr('transform_clear_time').not_exists()
        )

    def write_edge(self, edge):
        if not edge:
            return
        edge_entry = {
            'edge_label': edge.edge_label,
            'object_type': edge.edge_label,
            'internal_id': edge.internal_id,
            'from_object': edge.from_object,
            'to_object': edge.to_object,
            'object_properties': edge.object_properties
        }
        for property_name, object_property in edge.object_properties.items():
            edge_entry[property_name] = object_property
        try:
            self._table.put_item(
                Item=edge_entry,
                ConditionExpression=Attr('internal_id').not_exists()
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
        self.add_edge_to_vertex(edge.from_object, edge)
        self.add_edge_to_vertex(edge.to_object, edge, True)

    def add_edge_to_vertex(self, vertex_internal_id, edge, inbound=False):
        direction = "outbound"
        if inbound:
            direction = 'inbound'
        client = boto3.client('dynamodb')
        client.update_item(
            TableName=self._table_name,
            Key={'internal_id': {'S': vertex_internal_id}},
            UpdateExpression='ADD #direction.#edge  :internal',
            ExpressionAttributeValues={':internal': {'SS': [edge.internal_id]}},
            ExpressionAttributeNames={'#direction': direction, '#edge': edge.edge_label},
            ConditionExpression='NOT contains(#direction, :internal)'
        )

    def add_edge_type_to_vertex(self, vertex_internal_id, edge, direction):
        client = boto3.client('dynamodb')
        client.update_item(
            TableName=self._table_name,
            Key={'internal_id': {'S': vertex_internal_id}},
            UpdateExpression='ADD #direction.#edge = :empty',
            ExpressionAttributeNames={"#edge": edge.edge_label, '#direction': direction},
            ExpressionAttributeValues={':empty': {'L': []}}
        )

    def mark_ids_as_working(self, identifier_stem, id_values, object_type):
        already_working = []
        not_working = []
        for id_value in id_values:
            try:
                self._table.put_item(
                    Item={
                        'identifier_stem': identifier_stem,
                        'id_value': id_value,
                        'object_type': object_type,
                        'is_edge': False,
                        'completed': False,
                        'disposition': 'working',
                        'last_stage_seen': 'monitor',
                        'monitor_clear_time': datetime.datetime.now().timestamp(),
                        'last_seen_time': datetime.datetime.now().timestamp()
                    },
                    ConditionExpression=Attr('identifier_stem').not_exists() & Attr('id_value').not_exists()
                )
                not_working.append(id_value)
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise e
                already_working.append(id_value)
        return already_working, not_working

    def mark_object_as_blank(self, identifier_stem, id_value):
        self._table.update_item(
            Key={'identifier_stem': identifier_stem, 'id_value': id_value},
            UpdateExpression='SET completed = :c, disposition = :d, last_stage_seen = :s, last_seen_time = :t',
            ExpressionAttributeValues={
                ':c': True,
                ':d': 'blank',
                ':s': 'extraction',
                ':t': datetime.datetime.now()
            }
        )

    def mark_object_as_stage_cleared(self, identifier_stem, id_value, stage_name):
        self._table.update_item(
            Key={'identifier_stem': identifier_stem, 'id_value': id_value},
            UpdateExpression='SET last_stage_seen = :s, last_seen_time = :t, #sc = :t',
            ExpressionAttributeValues={
                ':s': stage_name,
                ':t': datetime.datetime.now().timestamp()
            },
            ExpressionAttributeNames={
                '#sc': f'{stage_name}_clear_time'
            },
            ConditionExpression=Attr(f'{stage_name}_clear_time').not_exists()
        )

    def add_stub_vertex(self, object_type, stub_properties, source_internal_id, rule_name):
        try:
            self._add_stub_vertex(object_type, stub_properties, source_internal_id, rule_name)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ValidationException':
                raise e
            self._add_stub_object_type(object_type, stub_properties, source_internal_id, rule_name)

    def add_vertex_properties(self, identifier_stem, id_value, vertex_properties):
        self._table.update_item(
            Key={'identifier_stem': identifier_stem, 'id_value': id_value},
            UpdateExpression='SET #v = :v, #o = :v',
            ExpressionAttributeValues={
                ':v': vertex_properties
            },
            ExpressionAttributeNames={
                '#v': 'vertex_properties',
                '#o': 'object_properties'
            },
            ConditionExpression=Attr('vertex_properties').not_exists() & Attr('object_properties').not_exists()
        )

    def _add_stub_vertex(self, object_type, stub_properties, source_internal_id, rule_name):
        stub = {
            'properties': stub_properties,
            'source_internal_id': source_internal_id,
            'rule_name': rule_name
        }
        self._table.update_item(
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
            'properties': stub_properties,
            'source_internal_id': source_internal_id,
            'rule_name': rule_name
        }
        self._table.update_item(
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
