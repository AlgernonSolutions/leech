import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex, IdentifierStem, VertexRegulator, PotentialEdge
from toll_booth.alg_obj.serializers import AlgEncoder


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
        object_type = kwargs.get('object_type', None)
        if identifier_stem:
            identifier_stem = IdentifierStem.from_raw(identifier_stem)
            object_type = identifier_stem.object_type
        self._identifier_stem = identifier_stem
        self._id_value = id_value
        self._dynamo_parameters = DynamoParameters(identifier_stem, id_value)
        self._object_properties = kwargs.get('object_properties', {})
        self._object_type = object_type

    @property
    def identifier_stem(self):
        return self._identifier_stem

    @property
    def object_type(self):
        return self._object_type

    @property
    def object_properties(self):
        return self._object_properties

    def for_seed(self, id_value):
        dynamo_parameters = DynamoParameters(self._identifier_stem, id_value)
        base = self._for_update('monitoring', is_initial=True)
        base['Key'] = dynamo_parameters.as_key
        base['UpdateExpression'] = base['UpdateExpression'] + ', #id=:id, #d=:d, #ot=:ot, #c=:c'
        base['ExpressionAttributeNames'].update({
            '#id': 'id_value',
            '#d': 'disposition',
            '#ot': 'object_type',
            '#c': 'completed'
        })
        base['ExpressionAttributeValues'].update({
            ':id': id_value,
            ':d': 'working',
            ':ot': self._object_type,
            ':c': False
        })
        return base

    @property
    def for_blank(self):
        base = self._for_update('extraction')
        base['UpdateExpression'] = base['UpdateExpression'] + ', #c=:c, #d=:d'
        base['ExpressionAttributeNames'].update({
            '#c': 'completed',
            '#d': 'disposition'
        })
        base['ExpressionAttributeValues'].update({
            ':c': True,
            ':d': 'blank'
        })
        return base

    @property
    def for_graphed(self):
        base = self._for_update('graphing')
        base['UpdateExpression'] = base['UpdateExpression'] + ', #d=:d'
        base['ExpressionAttributeNames']['#d'] = 'disposition'
        base['ExpressionAttributeValues'][':d'] = 'processing'
        return base

    @property
    def for_key(self):
        return {'Key': self._dynamo_parameters.as_key}

    def for_created_vertex(self, potential_vertex):
        base = self._for_update('assimilation', is_initial=True)
        base['UpdateExpression'] = base['UpdateExpression'] + ', #i=:i, #o=:v, #id=:id, #d=:d, #ot=:ot, #c=:c'
        base['ExpressionAttributeNames'].update({
            '#id': 'id_value',
            '#d': 'disposition',
            '#ot': 'object_type',
            '#i': 'internal_id',
            '#o': 'object_properties',
            '#c': 'completed'
        })
        base['ExpressionAttributeValues'].update({
            ':id': potential_vertex.id_value,
            ':d': 'graphing',
            ':ot': potential_vertex.object_type,
            ':i': potential_vertex.internal_id,
            ':v': self._clean_object_properties(potential_vertex.object_properties),
            ':c': False
        })
        base['ConditionExpression'] = base['ConditionExpression'] & Attr('identifier_stem').not_exists() & Attr(
            'sid_value').not_exists()
        return base

    def for_stub(self, potential_vertex):
        base = self._for_update('assimilation', is_initial=True)
        stub_parameters = self._calculate_stub_parameters(potential_vertex)
        expression_names = {
            '#d': 'disposition',
            '#ot': 'object_type',
            '#o': 'object_properties',
            '#c': 'completed'
        }
        expression_values = {
            ':d': 'graphing',
            ':ot': potential_vertex.object_type,
            ':v': self._clean_object_properties(potential_vertex.object_properties),
            ':c': False
        }
        update_expression = ', #o=:v, #d=:d, #ot=:ot, #c=:c'
        if potential_vertex.is_internal_id_set:
            update_expression += ', #i=:i'
            expression_names['#i'] = 'internal_id'
            expression_values[':i'] = potential_vertex.internal_id
        if potential_vertex.is_id_value_set:
            update_expression += ', #id=:id'
            expression_names['#id'] = 'id_value'
            expression_values[':id'] = potential_vertex.id_value
        base['Key'] = stub_parameters.as_key
        base['UpdateExpression'] = base['UpdateExpression'] + update_expression
        base['ExpressionAttributeNames'].update(expression_names)
        base['ExpressionAttributeValues'].update(expression_values)
        base['ConditionExpression'] = base['ConditionExpression'] & Attr('identifier_stem').not_exists() & Attr(
            'sid_value').not_exists()
        return base

    def for_extraction(self, extracted_data):
        base = self._for_update('extraction')
        base['UpdateExpression'] = base['UpdateExpression'] + ', #ex=:ex'
        base['ExpressionAttributeValues'][':ex'] = json.dumps(extracted_data, cls=AlgEncoder)
        base['ExpressionAttributeNames']['#ex'] = 'extracted_data'
        return base

    def for_transformation(self, vertex, potentials):
        disposition = 'working'
        if not potentials:
            disposition = 'graphing'
        base = self._for_update('transformation')
        base['UpdateExpression'] = base['UpdateExpression'] + ', #i=:i, #o=:v, #d=:d, #ps=:ps, #p.#as=:e'
        base['ExpressionAttributeNames'].update({
            '#i': 'internal_id',
            '#o': 'object_properties',
            '#d': 'disposition',
            '#ps': 'potentials',
            '#as': 'assimilation'
        })
        base['ExpressionAttributeValues'].update({
            ':i': vertex.internal_id,
            ':v': self._clean_object_properties(vertex.object_properties),
            ':d': disposition,
            ':ps': self._format_potentials(potentials),
            ':e': {}
        })
        return base

    def for_assimilation(self, ruled_edge_type, assimilation_results):
        base = self._for_update('assimilation')
        base['UpdateExpression'] = base['UpdateExpression'] + ', #ps.#re.#iv=:iv, #ps.#re.#a=:a'
        base['UpdateExpression'] = base['UpdateExpression'].replace('#p.#s', '#p.#s.#re')
        base['ExpressionAttributeNames'].update({
            '#iv': 'identified_vertexes',
            '#a': 'assimilated',
            '#re': ruled_edge_type,
            '#ps': 'potentials'
        })
        # noinspection PyTypeChecker
        base['ExpressionAttributeValues'].update({
            ':iv': self._format_assimilation_results(assimilation_results),
            ':a': True
        })
        base['ConditionExpression'] = Attr(f'progress.assimilation.{ruled_edge_type}').not_exists()
        return base

    def _for_update(self, stage_name, is_initial=False):
        now = self._get_decimal_timestamp()
        update_expression = 'SET #lss=:s, #lts=:t, #p.#s=:p'
        attribute_names = {
            '#p': 'progress',
            '#s': stage_name,
            '#lss': 'last_stage_seen',
            '#lts': 'last_time_seen'
        }
        attribute_values = {
            ':t': now,
            ':s': stage_name,
            ':p': now
        }
        if is_initial:
            update_expression = update_expression.replace('#p.#s', "#p")
            attribute_names['#p'] = 'progress'
            attribute_values[':p'] = {stage_name: now}
            del attribute_names['#s']
        update_args = {
            'Key': self._dynamo_parameters.as_key,
            'UpdateExpression': update_expression,
            'ExpressionAttributeNames': attribute_names,
            'ExpressionAttributeValues': attribute_values,
            'ConditionExpression': Attr(f'progress.{stage_name}').not_exists()
        }
        return update_args

    @classmethod
    def _calculate_stub_parameters(cls, potential_vertex):
        return DynamoParameters(
            cls._calculate_stub_identifier_stem(potential_vertex),
            cls._calculate_stub_sid(potential_vertex)
        )

    @classmethod
    def _calculate_stub_identifier_stem(cls, potential_vertex):
        if potential_vertex.is_identifier_stem_set:
            if potential_vertex.is_identifiable:
                return potential_vertex.identifier_stem
            return potential_vertex.as_stub_for_object
        return IdentifierStem.for_stub(potential_vertex)

    @classmethod
    def _calculate_stub_sid(cls, potential_vertex):
        if potential_vertex.is_id_value_set:
            return str(potential_vertex.id_value)
        object_properties = cls._clean_object_properties(potential_vertex.object_properties, for_json=True)
        return json.dumps(object_properties)

    @classmethod
    def _get_decimal_timestamp(cls):
        import datetime
        return Decimal(datetime.datetime.now().timestamp())

    @classmethod
    def _clean_object_properties(cls, object_properties, for_json=False):
        cleaned = {}
        for property_name, object_property in object_properties.items():
            if object_property == '':
                object_property = None
            if hasattr(object_property, 'is_missing'):
                object_property = None
            if isinstance(object_property, datetime):
                object_property = object_property.timestamp()
            if for_json:
                object_property = str(object_property)
            cleaned[property_name] = object_property
        return cleaned

    @classmethod
    def _format_potentials(cls, potentials):
        formatted = {}
        for potential in potentials:
            rule_entry = potential[1]
            potential_vertex = potential[0]
            formatted[rule_entry.edge_type] = {
                'rule_entry': cls._format_rule_entry(rule_entry),
                'potential_vertex': cls._format_potential_vertex(potential_vertex),
                'assimilated': False
            }
        return formatted

    @classmethod
    def _format_assimilation_results(cls, assimilation_results):
        formatted = []
        for entry in assimilation_results:
            formatted.append({
                'edge': cls._format_potential_edge(entry['edge']),
                'vertex': cls._format_potential_vertex(entry['vertex'])
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
            'identifier_stem': str(potential_vertex.identifier_stem),
            'sid_value': str(potential_vertex.id_value),
            'id_value': potential_vertex.id_value,
            'internal_id': potential_vertex.internal_id,
            'object_properties': object_properties,
            'object_type': potential_vertex.object_type
        }

    @classmethod
    def _format_potential_edge(cls, potential_edge):
        object_properties = {}
        for property_name, object_property in potential_edge.object_properties.items():
            if object_property == '' or hasattr(object_property, 'is_missing'):
                object_property = None
            object_properties[property_name] = object_property
        return {
            'identifier_stem': str(potential_edge.identifier_stem),
            'sid_value': str(potential_edge.id_value),
            'internal_id': potential_edge.internal_id,
            'object_properties': object_properties,
            'from_object': potential_edge.from_object,
            'to_object': potential_edge.to_object,
            'edge_label': potential_edge.edge_label
        }

    @classmethod
    def _format_rule_entry(cls, rule_entry):
        return {
            'target_type': rule_entry.target_type,
            'edge_type': rule_entry.edge_type,
            'target_constants': rule_entry.target_constants,
            'target_specifiers': cls._format_specifiers(rule_entry.target_specifiers),
            'if_absent': rule_entry.if_absent,
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


class EmptyIndexException(Exception):
    pass


class MissingExtractionInformation(Exception):
    pass


class DynamoParameters:
    def __init__(self, partition_key_value, sort_key_value, **kwargs):
        partition_key_name = kwargs.get('partition_key_name', os.getenv('PARTITION_KEY', 'sid_value'))
        sort_key_name = kwargs.get('sort_key_name', os.getenv('SORT_KEY', 'identifier_stem'))
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


class LeechDriver:
    _internal_id_index = os.getenv('INTERNAL_ID_INDEX', 'internal_ids')
    _id_value_index = os.getenv('ID_VALUE_INDEX', 'id_values')

    def __init__(self, **kwargs):
        table_name = kwargs.get('table_name', os.getenv('TABLE_NAME', 'GraphObjects'))
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)

    @leeched
    def get_object(self, leech_record):
        results = self._table.get_item(**leech_record.for_key)
        try:
            vertex_information = results['Item']
        except KeyError:
            return None
        object_type = vertex_information['object_type']
        regulator = VertexRegulator.get_for_object_type(object_type)
        source_vertex = regulator.create_potential_vertex(vertex_information['object_properties'])
        other_vertexes = []
        edges = []
        potentials = vertex_information.get('potentials', {})
        for edge_label, potential in potentials.items():
            identified_vertexes = potential.get('identified_vertexes', [])
            identified_vertex_regulator = None
            for identified in identified_vertexes:
                identified_vertex = identified['vertex']
                if not identified_vertex_regulator:
                    identified_vertex_regulator = VertexRegulator.get_for_object_type(identified_vertex['object_type'])
                other_potential_vertex = identified_vertex_regulator.create_potential_vertex(identified_vertex['object_properties'])
                other_vertexes.append(other_potential_vertex)
                potential_edge = PotentialEdge.from_json(identified['edge'])
                edges.append(potential_edge)
        return {
            'source': source_vertex,
            'others': other_vertexes,
            'edges': edges
        }

    @leeched
    def put_vertex_seed(self, id_value, leech_record):
        return self._table.update_item(**leech_record.for_seed(id_value))

    @leeched
    def mark_ids_as_working(self, id_values, leech_record):
        already_working = []
        not_working = []
        for id_value in id_values:
            try:
                self._table.update_item(**leech_record.for_seed(id_value))
                not_working.append(id_value)
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise e
                already_working.append(id_value)
        return already_working, not_working

    @leeched
    def mark_object_as_blank(self, leech_record):
        return self._table.update_item(**leech_record.for_blank)

    @leeched
    def mark_object_as_graphed(self, leech_record):
        logging.info(
            'started a mark_object_as_graphed execution, '
            'leech record to be executed is: %s' % leech_record.for_graphed)
        results = self._table.update_item(**leech_record.for_graphed)
        logging.info('completed the leech call')
        return results

    @leeched
    def set_extraction_results(self, extracted_data, leech_record):
        return self._table.update_item(**leech_record.for_extraction(extracted_data))

    @leeched
    def set_transform_results(self, vertex, potentials, leech_record):
        if not vertex.is_identifiable:
            raise RuntimeError(
                f'could not uniquely identify a ruled vertex for type: {vertex.object_type}')
        if not vertex.is_properties_complete:
            raise RuntimeError(
                f'could not derive all properties for ruled vertex type: {vertex.object_type}')
        return self._table.update_item(**leech_record.for_transformation(vertex, potentials))

    @leeched
    def set_assimilation_results(self, ruled_edge_type, assimilation_results, leech_record):
        return self._table.update_item(**leech_record.for_assimilation(ruled_edge_type, assimilation_results))

    @leeched
    def set_assimilated_vertex(self, potential_vertex, is_stub, leech_record):
        if is_stub is True:
            return self._table.update_item(**leech_record.for_stub(potential_vertex))
        return self._table.update_item(**leech_record.for_created_vertex(potential_vertex))

    def get_extractor_function_names(self, identifier_stem):
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        params = DynamoParameters(identifier_stem.for_dynamo, identifier_stem)
        results = self._table.get_item(
            Key=params.as_key
        )
        try:
            extractor_function_names = results['Item']['extractor_function_names']
            return extractor_function_names
        except KeyError:
            raise MissingExtractionInformation(
                'could not find extractor information for identifier stem %s' % identifier_stem)

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
        logging.info('completed a scan of the data space to find potential vertexes with properties: %s '
                     'returned the raw values of: %s' % (vertex_properties, potential_vertexes))
        return [PotentialVertex.from_json(x) for x in potential_vertexes]

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


class UtilityDriver:
    _internal_id_index = os.getenv('INTERNAL_ID_INDEX', 'internal_ids')
    _id_value_index = os.getenv('ID_VALUE_INDEX', 'id_values')

    def __init__(self, **kwargs):
        table_name = kwargs.get('table_name', os.getenv('TABLE_NAME', 'GraphObjects'))
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)

    def delete_vertex(self, identifier_stem, id_value):
        dynamo_params = DynamoParameters(identifier_stem, id_value)
        return self._table.delete_item(
            Key=dynamo_params.as_key
        )

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
