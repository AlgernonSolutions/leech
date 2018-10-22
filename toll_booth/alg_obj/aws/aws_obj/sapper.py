import os

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph import InternalId


class SchemaWhisperer:
    def __init__(self, table_name=None):
        if not table_name:
            try:
                table_name = os.environ['SCHEMA_TABLE']
            except KeyError:
                table_name = 'Schema'
        self._table_name = table_name
        dynamo = boto3.resource('dynamodb')
        self._table_resource = dynamo.Table(table_name)
        self._client = boto3.client('dynamodb')

    def get_schema_entry(self, entry_name, version=None):
        if not version:
            current_query = self._table_resource.query(
                Limit=1,
                ScanIndexForward=False,
                KeyConditionExpression=Key('entry_name').eq(entry_name)
            )
            return current_query
        current_query = self._table_resource.get_item(
            Key={
                'entry_name': entry_name,
                'version': version
            }
        )
        return current_query

    def get_schema(self):
        schema_returned = {
            'edge': [],
            'vertex': []
        }
        max_values = {}
        scan_paginator = self._client.get_paginator('scan')
        pagination_results = scan_paginator.paginate(
            TableName=self._table_name,
            ProjectionExpression='entry_name, version'
        )
        for entry in pagination_results:
            for item in entry['Items']:
                entry_name = item['entry_name']['S']
                version = int(item['version']['N'])
                if entry_name not in max_values:
                    max_values[entry_name] = {'entry_name': entry_name, 'version': version}
                else:
                    current_max = max_values[entry_name]['version']
                    if version > current_max:
                        max_values[entry_name]['version'] = version
        results = [self.get_schema_entry(x['entry_name'], x['version'])['Item'] for x in max_values.values()]
        for result in results:
            if 'vertex_name' in result:
                schema_returned['vertex'].append(result)
            else:
                schema_returned['edge'].append(result)
        return schema_returned

    def get_rules(self, entry_name, version=None):
        from boto3.dynamodb.conditions import Key
        if not version:
            current_query = self._table_resource.query(
                Limit=1,
                ScanIndexForward=False,
                KeyConditionExpression=Key('entry_name').eq(entry_name),
                ProjectionExpression='#r',
                ExpressionAttributeNames={'#r': 'rules'}
            )
            return current_query
        current_query = self._table_resource.get_item(
            Key={
                'entry_name': entry_name,
                'version': version
            }
        )
        return current_query

    def write_schema_entry(self, schema_entry):
        entry_name = self._parse_entry_name(schema_entry)
        schema_entry['entry_name'] = entry_name
        current_query = self.get_schema_entry(entry_name)
        try:
            current_version = current_query['Items'][0]['version']
            current_entry = current_query['Items'][0]
        except IndexError:
            schema_entry['version'] = 1
            self._batch_writer.put_item(
                Item=schema_entry
            )
            return
        schema_entry['version'] = current_version
        if schema_entry != current_entry:
            schema_entry['version'] += 1
            self._batch_writer.put_item(
                Item=schema_entry
            )

    @staticmethod
    def _parse_entry_name(schema_entry):
        entry_names = ['vertex_name', 'edge_label', 'cluster_name']
        for entry_name in entry_names:
            try:
                return schema_entry[entry_name]
            except KeyError:
                continue
        raise KeyError()

    def __enter__(self):
        self._batch_writer = self._table_resource.batch_writer()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type and not exc_val:
            self._batch_writer.__exit__(exc_type, exc_val, exc_tb)
            return True
        import traceback
        traceback.print_exc()
        print(exc_type)


class SecretWhisperer:
    _sensitive_table_name = os.getenv('SENSITIVE_TABLE', 'Sensitives')

    @classmethod
    def get_secret(cls, internal_id):
        resource = boto3.resource('dynamodb')
        table = resource.Table(cls._sensitive_table_name)
        results = table.query(
            IndexName='string',
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=Key('insensitive').eq(internal_id)
        )
        print(results)

    @classmethod
    def put_secret(cls, sensitive_property, source_internal_id, data_type):
        resource = boto3.resource('dynamodb')
        table = resource.Table(cls._sensitive_table_name)
        id_string = ''.join([source_internal_id, data_type])
        internal_id = InternalId(id_string).id_value
        try:
            table.update_item(
                Key={'insensitive': internal_id},
                UpdateExpression='SET sensitive_entry = if_not_exists(sensitive_entry, :s)',
                ExpressionAttributeValues={':s': sensitive_property},
                ReturnValues='NONE'
            )
        except ClientError as e:
            print(e)
        return internal_id


class DynamoDriver:
    def __init__(self, table_name=None):
        if not table_name:
            table_name = 'GraphObjects'
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)

    def write_vertex(self, vertex):
        vertex_entry = {
            'internal_id': vertex.internal_id,
            'object_type': vertex.object_type,
            'vertex_name': vertex.object_type,
            'is_stub': vertex.is_stub,
            'inbound': {},
            'outbound': {}
        }
        for object_property_name, object_property in vertex.object_properties.items():
            vertex_entry[object_property_name] = object_property
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
            'to_object': edge.to_object
        }
        for object_property_name, object_property in edge.object_properties.items():
            edge_entry[object_property_name] = object_property
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
