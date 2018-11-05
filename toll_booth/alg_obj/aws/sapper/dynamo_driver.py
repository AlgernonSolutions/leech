import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex, PotentialEdge, IdentifierStem


def leeched(production_function):
    def wrapper(*args, **kwargs):
        if 'leech_record' in kwargs:
            return production_function(*args, **kwargs)
        identifier_stem = kwargs.get('identifier_stem')
        id_value = kwargs.get('id_value')
        vertex_properties = kwargs.get('vertex_properties', {})
        record = LeechRecord(identifier_stem, id_value, vertex_properties=vertex_properties)
        return production_function(*args, leech_record=record)
    return wrapper


class LeechRecord:
    def __init__(self, identifier_stem, id_value, **kwargs):
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        self._identifier_stem = identifier_stem
        self._id_value = id_value
        self._dynamo_parameters = DynamoParameters(identifier_stem, id_value)
        self._object_properties = kwargs.get('object_properties', {})
        self._object_type = identifier_stem.object_type

    def vertex_data(self, stage_name):
        now = self._get_decimal_timestamp()
        return {
            'id_value': self._id_value,
            'disposition': 'working',
            'last_stage_seen': stage_name,
            'object_type': self._object_type,
            'last_time_seen': now,
            'completed': False,
            'progress': {stage_name: now}
        }

    @property
    def identifier_stem(self):
        return self._identifier_stem

    @property
    def object_type(self):
        return self._object_type

    @property
    def object_properties(self):
        return self._object_properties

    @property
    def for_seed(self):
        seed = self._dynamo_parameters.as_key
        seed.update(self.vertex_data('monitoring'))
        return {
            'Item': seed,
            'ConditionExpression': self._dynamo_parameters.as_no_overwrite
        }

    @property
    def for_blank(self):
        set_command = 'SET #c=:c, #d=:d, #lss=:s, #lts=:t'
        return {
            'Key': self._dynamo_parameters.as_key,
            'UpdateExpression': set_command,
            'ExpressionAttributeValues': {
                ':c': True,
                ':d': 'blank',
                ':t': self._get_decimal_timestamp(),
                ':s': 'transformation'
            },
            'ExpressionAttributeNames': {
                '#c': 'completed',
                '#d': 'disposition',
                '#lss': 'last_stage_seen',
                '#lts': 'last_time_seen'
            },
            'ConditionExpression': Attr('disposition').ne('blank')
        }

    def for_update(self):
        return {
            'Key': self._dynamo_parameters.as_key,
            'UpdateExpression': 'SET ',
            'ExpressionAttributeValues': {

            }
        }

    @classmethod
    def _get_decimal_timestamp(cls):
        import datetime
        return Decimal(datetime.datetime.now().timestamp())


class EmptyIndexException(Exception):
    pass


class DynamoParameters:
    def __init__(self, partition_key_value, sort_key_value, **kwargs):
        partition_key_name = kwargs.get('partition_key_name', os.getenv('PARTITION_KEY', 'identifier_stem'))
        sort_key_name = kwargs.get('sort_key_name', os.getenv('SORT_KEY', 'sid_value'))
        self._partition_key_name = partition_key_name
        self._partition_key_value = partition_key_value
        self._sort_key_name = sort_key_name
        self._sort_key_value = sort_key_value

    @classmethod
    def for_stub(cls):
        return cls('stub', '0')

    @property
    def as_key(self):
        return {
            self._partition_key_name: str(self._partition_key_value),
            self._sort_key_name: str(self._sort_key_value)
        }

    @property
    def as_no_overwrite(self):
        return Attr(self._partition_key_name).not_exists() & Attr(self._sort_key_name).not_exists()


class DynamoDriver:
    _internal_id_index = os.getenv('INTERNAL_ID_INDEX', 'internal_ids')
    _id_value_index = os.getenv('ID_VALUE_INDEX', 'id_values')

    def __init__(self, **kwargs):
        table_name = kwargs.get('table_name', os.getenv('TABLE_NAME', 'GraphObjects'))
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)

    def clear_identifier_stem(self, identifier_stem):
        identified_values = self.get_identifier_stem(identifier_stem)
        total = len(identified_values)
        progress = 1
        with self._table.batch_writer() as batch:
            for identified_value in identified_values:
                dynamo_params = DynamoParameters(identified_value['identifier_stem'], identified_value['sid_value'])
                batch.delete_item(Key=dynamo_params.as_key)
                print(f'{progress}/{total}')
                progress += 1

    def get_identifier_stem(self, identifier_stem):
        results, token = self._get_identifier_stem(identifier_stem)
        while token:
            results.extend(self._get_identifier_stem(identifier_stem, token))
        return results

    def _get_identifier_stem(self, identifier_stem, token=None):
        identifier_args = {
            'KeyConditionExpression': Key('identifier_stem').eq(str(identifier_stem))
        }
        if token:
            identifier_args['ExclusiveStartKey'] = token
        results = self._table.query(**identifier_args)
        return results['Items'], results.get('LastEvaluatedKey', None)

    def get_extractor_function_names(self, identifier_stem):
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        params = DynamoParameters(identifier_stem.for_dynamo, identifier_stem)
        results = self._table.get_item(
            Key=params.as_key
        )
        extractor_function_names = results['Item']['extractor_function_names']
        return extractor_function_names

    def get_object(self, identifier_stem, id_value):
        if '#edge#' in identifier_stem:
            return self.get_edge(identifier_stem, id_value)
        if '#vertex#' in identifier_stem:
            return self.get_vertex(identifier_stem, id_value)
        raise RuntimeError(f'could not ascertain the object type (vertex/edge) from identifier: {identifier_stem}')

    def query_index_value_max(self, identifier_stem, index_name=None):
        if not index_name:
            index_name = self._id_value_index
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
            raise EmptyIndexException

    def find_potential_vertexes(self, object_type, vertex_properties):
        potential_vertexes, token = self._scan_vertexes(object_type, vertex_properties)
        while token:
            more_vertexes, token = self._scan_vertexes(object_type, vertex_properties, token)
            potential_vertexes.extend(more_vertexes)
        return [PotentialVertex.from_json(x) for x in potential_vertexes]

    def _scan_vertexes(self, object_type, vertex_properties, token=None):
        filter_properties = [f'begins_with(identifier_stem, :is)']
        expression_names = {}
        expression_values = {
            ':is': f'#vertex#{object_type}#'
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

    @leeched
    def put_vertex_seed(self, leech_record):
        return self._table.put_item(leech_record.for_seed)

    def get_vertex(self, identifier_stem, id_value):
        params = DynamoParameters(identifier_stem, id_value)
        results = self._table.get_item(
            Key=params.as_key
        )
        try:
            vertex_information = results['Item']
        except KeyError:
            return None
        return PotentialVertex.from_json(vertex_information)

    def set_transform_results(self, vertex, potentials):
        if not vertex.is_identifiable:
            raise RuntimeError(
                f'could not uniquely identify a ruled vertex for type: {vertex.object_type}')
        if not vertex.is_properties_complete:
            raise RuntimeError(
                f'could not derive all properties for ruled vertex type: {vertex.object_type}')
        stage_name = 'transformation'
        update_expression = 'SET #i=:i, #o=:v, #d=:d, #lts=:t, #sc=:t, #lss=:lss, #p=:p'
        params = DynamoParameters(vertex.identifier_stem, vertex.id_value)
        object_properties = {}
        for property_name, object_property in vertex.object_properties.items():
            if object_property == '':
                object_property = None
            object_properties[property_name] = object_property
        return self._table.update_item(
            Key=params.as_key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues={
                ':v': object_properties,
                ':i': vertex.internal_id,
                ':d': 'graphing',
                ':t': self._get_decimal_timestamp(),
                ':lss': stage_name,
                ':p': self._format_potentials(potentials)
            },
            ExpressionAttributeNames={
                '#i': 'internal_id',
                '#o': 'object_properties',
                '#d': 'disposition',
                '#lts': 'last_time_seen',
                '#sc': f'progress.{stage_name}',
                '#lss': 'last_stage_seen',
                '#p': 'potentials'
            },
            ConditionExpression=Attr(f'progress.{stage_name}').not_exists()
        )

    def write_edge(self, edge, stage_name):
        now = self._get_decimal_timestamp()
        params = DynamoParameters(edge.identifier_stem, edge.internal_id)
        edge_entry = params.as_key
        edge_entry.update({
            'object_type': edge.edge_label,
            'internal_id': edge.internal_id,
            'from_object': edge.from_object,
            'to_object': edge.to_object,
            'object_properties': edge.object_properties,
            'disposition': 'graphing',
            'completed': False,
            'last_stage_seen': stage_name,
            f'{stage_name}_clear_time': now,
            'last_seen_time': now
        })
        try:
            return self._table.put_item(
                Item=edge_entry,
                ConditionExpression=params.as_no_overwrite
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e

    def get_edge(self, identifier_stem, edge_internal_id):
        results = self._table.get_item(Key=DynamoParameters(identifier_stem, edge_internal_id).as_key)
        return PotentialEdge.from_json(results['Item'])

    @leeched
    def mark_ids_as_working(self, id_values, leech_record):
        already_working = []
        not_working = []
        for id_value in id_values:
            try:
                self.put_vertex_seed(leech_record=leech_record)
                not_working.append(id_value)
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise e
                already_working.append(id_value)
        return already_working, not_working

    @leeched
    def mark_object_as_blank(self, leech_record):
        return self._table.update_item(leech_record.for_blank)

    def mark_object_as_stage_cleared(self, identifier_stem, id_value, stage_name):
        return self._table.update_item(
            Key=DynamoParameters(identifier_stem, id_value).as_key,
            UpdateExpression='SET last_stage_seen = :s, last_time_seen = :t, #sc = :t',
            ExpressionAttributeValues={
                ':s': stage_name,
                ':t': self._get_decimal_timestamp()
            },
            ExpressionAttributeNames={
                '#sc': f'progress.{stage_name}'
            },
            ConditionExpression=Attr(f'progress.{stage_name}').not_exists()
        )

    @leeched
    def mark_object_as_graphed(self, identifier_stem, id_value):
        stage_name = 'graphing'
        return self._table.update_item(
            Key=DynamoParameters(identifier_stem, id_value).as_key,
            UpdateExpression='SET last_stage_seen = :s, last_seen_time = :t, #sc = :t, disposition = :d',
            ExpressionAttributeValues={
                ':s': stage_name,
                ':t': self._get_decimal_timestamp(),
                ':d': 'processing'
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
            Key=DynamoParameters(identifier_stem, id_value).as_key,
            UpdateExpression=update_components[2],
            ExpressionAttributeValues=update_components[1],
            ExpressionAttributeNames=update_components[0],
            ConditionExpression=Attr('object_properties').not_exists()
        )

    def set_extracted_data(self, identifier_stem, id_value, extracted_data):
        params = DynamoParameters(identifier_stem, id_value)
        return self._table.update_item(
            Key=params.as_key,
            UpdateExpression='SET extracted_data = :ex, #p = :t, #lss = :t, #lts = :s',
            ExpressionAttributeValues={
                ':ex': extracted_data,
                ':t': self._get_decimal_timestamp(),
                ':s': 'extraction'
            },
            ExpressionAttributeNames={
                '#p': 'progress.extraction',
                '#lss': 'last_stage_seen',
                '#lts': 'last_time_seen'
            },
            ConditionExpression=Attr('extracted_data').not_exists()
        )

    def _add_stub_vertex(self, object_type, stub_properties, source_internal_id, rule_name):
        object_properties = {x: y for x, y in stub_properties.items() if not hasattr(y, 'is_missing')}
        stub = {
            'object_properties': object_properties,
            'source_internal_id': source_internal_id,
            'rule_name': rule_name
        }
        return self._table.update_item(
            Key=DynamoParameters.for_stub().as_key,
            UpdateExpression='SET #ot = list_append(#ot, :s)',
            ExpressionAttributeValues={
                ':s': [stub]
            },
            ExpressionAttributeNames={
                '#ot': object_type
            }
        )

    def _add_stub_object_type(self, object_type, stub_properties, source_internal_id, rule_name):
        object_properties = {x: y for x, y in stub_properties.items() if not hasattr(y, 'is_missing')}
        stub = {
            'object_properties': object_properties,
            'source_internal_id': source_internal_id,
            'rule_name': rule_name
        }
        return self._table.update_item(
            Key=DynamoParameters.for_stub().as_key,
            UpdateExpression='SET #ot = :s',
            ExpressionAttributeValues={
                ':s': [stub]
            },
            ExpressionAttributeNames={
                '#ot': object_type
            },
            ConditionExpression=Attr(object_type).not_exists()
        )

    def _format_potentials(self, potentials):
        formatted = []
        for potential in potentials:
            formatted.append({
                'rule_entry': self._format_rule_entry(potential[1]),
                'potential_vertex': self._format_potential_vertex(potential[0])
            })
        return formatted

    @classmethod
    def _format_potential_vertex(cls, potential_vertex):
        object_properties = {}
        for property_name, object_property in potential_vertex.object_properties.items():
            if object_property == '' or hasattr(object_property, 'is_missing'):
                object_property = None
            object_properties[property_name] = object_property
        return {
            'identifier_stem': potential_vertex.identifier_stem,
            'sid_value': str(potential_vertex.id_value),
            'id_value': potential_vertex.id_value,
            'internal_id': potential_vertex.internal_id,
            'object_properties': object_properties
        }

    @classmethod
    def _format_rule_entry(cls, rule_entry):
        return {
            'target_type': rule_entry.target_type,
            'edge_type': rule_entry.edge_type,
            'target_constants': rule_entry.target_constants,
            'target_specifiers': cls._format_specifiers(rule_entry.target_specifiers),
            'if_absent': rule_entry.if_missing,
            'inbound': rule_entry.inbound
        }

    @classmethod
    def _format_specifiers(cls, target_specifiers):
        return [cls._format_specifier(x) for x in target_specifiers]

    @classmethod
    def _format_specifier(cls, target_specifier):
        return {
            'specifier_name': target_specifier.specifier_name,
            'specifier_type': target_specifier.specifier_type,
            'shared_properties': getattr(target_specifier, 'shared_properties', None),
            'extracted_properties': getattr(target_specifier, 'extracted_properties', None),
            'function_name': getattr(target_specifier, 'function_name', None)
        }

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
