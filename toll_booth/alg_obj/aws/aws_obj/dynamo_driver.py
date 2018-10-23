import datetime
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex


class DynamoDriver:
    def __init__(self, table_name=None):
        if not table_name:
            table_name = 'GraphObjects'
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)

    def query_index_max(self, object_type, id_source, id_type, index_name=None):
        if not index_name:
            index_name = 'object_type-id_value-index'
        results = self._table.query(
            IndexName=index_name,
            TableName=self._table_name,
            Limit=1,
            Select='ALL_PROJECTED_ATTRIBUTES',
            ScanIndexForward=False,
            KeyConditionExpression=Key('object_type').eq(object_type),
            FilterExpression=Attr('id_source').eq(id_source) & Attr('id_type').eq(id_type)
        )
        print(results)

    def find_potential_vertexes(self, vertex_properties):
        formatted_vertexes = []
        potential_vertexes, token = self._scan_vertexes(vertex_properties)
        while token:
            more_vertexes, token = self._scan_vertexes(vertex_properties)
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
                    object_properties, potential_vertex['is_stub'])
            )
        return formatted_vertexes

    def _scan_vertexes(self, vertex_properties, token=None):
        filter_properties = []
        expression_names = {}
        expression_values = {}
        pointer = 1
        for property_name, vertex_property in vertex_properties.items():
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
        vertex_entry = {
            'internal_id': vertex.internal_id,
            'object_type': vertex.object_type,
            'vertex_name': vertex.object_type,
            'is_stub': vertex.is_stub,
            'object_properties': vertex.object_properties,
            'inbound': {},
            'outbound': {}
        }
        for property_name, object_property in vertex.object_properties.items():
            vertex_entry[property_name] = object_property
        return self._table.put_item(
            Item=vertex_entry,
            ConditionExpression=Attr('internal_id').not_exists()
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


class DynamiteSapper:
    def __init__(self, table_name=None):
        if not table_name:
            table_name = 'Fuses'
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)

    def check_if_id_working(self, internal_id):
        results = self._table.get_item(
            Key={'internal_id': internal_id}
        )
        if results['Item']:
            expiration_timestamp = results['Item']['expiration']
            if datetime.datetime.now() <= datetime.datetime.fromtimestamp(expiration_timestamp):
                return True
        return False

    def mark_id_as_working(self, internal_id, ttl_hours=0, ttl_minutes=0, ttl_seconds=0):
        ttl = datetime.timedelta(hours=ttl_hours, minutes=ttl_minutes, seconds=ttl_seconds)
        if not ttl_hours and not ttl_minutes and not ttl_seconds:
            ttl = datetime.timedelta(hours=1)
        expiration_date = datetime.datetime.now() + ttl
        self._table.put_item(
            Item={
                'internal_id': internal_id,
                'expiration': expiration_date.timestamp()
            }
        )
