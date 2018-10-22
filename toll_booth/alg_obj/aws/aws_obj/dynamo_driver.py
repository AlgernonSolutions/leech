from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex


class DynamoDriver:
    def __init__(self, table_name=None):
        if not table_name:
            table_name = 'GraphObjects'
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)

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