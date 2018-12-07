import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex, IdentifierStem, VertexRegulator, PotentialEdge
from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder


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
        self._dynamo_parameters = DynamoParameters(id_value, identifier_stem)
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

    @property
    def as_seed(self):
        return self.for_seed(self._id_value)

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

    def for_vertex_driven_seed(self, extracted_data):
        base = self._for_update('propagation', is_initial=True)
        id_value = self._id_value
        if isinstance(id_value, datetime):
            id_value = Decimal(id_value.timestamp())
        base['Key']['sid_value'] = str(id_value)
        base['UpdateExpression'] = base['UpdateExpression'] + ', #id=:id, #d=:d, #ot=:ot, #c=:c, #ex=:ex'
        base['ExpressionAttributeNames'].update({
            '#id': 'id_value',
            '#d': 'disposition',
            '#ot': 'object_type',
            '#c': 'completed',
            '#ex': 'extracted_data'
        })
        base['ExpressionAttributeValues'].update({
            ':id': id_value,
            ':d': 'working',
            ':ot': self._object_type,
            ':c': False,
            ':ex': json.dumps(extracted_data, cls=AlgEncoder)
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

    def for_link_object(self, linked_internal_id, id_source, is_unlink):
        now = self._get_decimal_timestamp()
        base = self._for_update('linking', is_initial=True)
        paired_identifiers = {
            'linked_id_source': id_source,
            'internal_id': linked_internal_id
        }
        base['Key'] = DynamoParameters(now, IdentifierStem('vertex', 'link', paired_identifiers)).as_key
        base['UpdateExpression'] = base['UpdateExpression'] + ', #d=:d, #ids=:ids, #lt=:lt, #iul=:iul, #idv=:lt, #ot=:ot, #li=:li'
        base['ExpressionAttributeNames'].update({
            '#ids': 'linked_id_source',
            '#lt': 'utc_link_time',
            '#iul': 'is_unlink',
            '#d': 'disposition',
            '#idv': 'id_value',
            '#ot': 'object_type',
            '#li': 'linked_internal_id'
        })
        base['ExpressionAttributeValues'].update({
            ':ids': id_source,
            ':lt': now,
            ':iul': is_unlink,
            ':d': 'graphing',
            ':ot': 'link',
            ':li': linked_internal_id
        })
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


class MissingFieldValuesException(Exception):
    pass


class ObjectAlreadyWorkingException(Exception):
    pass


class MissingObjectException(Exception):
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
    _field_value_index = os.getenv('FIELD_VALUE_INDEX', 'field_values')
    _links_index = os.getenv('LINKS_INDEX', 'links')
    _credible_change_index = os.getenv('CREDIBLE_CHANGES', 'credible_change_stems')
    _creep_id_value_index = os.getenv('CREEP_ID_VALUES', 'creep_id_values')

    def __init__(self, **kwargs):
        table_name = kwargs.get('table_name', os.getenv('TABLE_NAME', 'VdGraphObjects'))
        self._table_name = table_name
        self._table = boto3.resource('dynamodb').Table(self._table_name)

    @property
    def table_name(self):
        return self._table_name

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
    def put_vertex_driven_seed(self, extracted_data, leech_record):
        try:
            update_args = leech_record.for_vertex_driven_seed(extracted_data)
            self._table.update_item(**update_args)
            return False
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
            return True

    @leeched
    def mark_id_as_working(self, leech_record):
        try:
            self._table.update_item(**leech_record.as_seed)
            return False
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
            return True

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

    @leeched
    def set_link_object(self, linked_internal_id, id_source, is_unlink, leech_record):
        return self._table.update_item(
            **leech_record.for_link_object(linked_internal_id, id_source, is_unlink)
        )

    def get_credible_id_name(self, id_type):
        table_identifier_stem = IdentifierStem('vertex', 'CredibleTable', {'table_name': id_type})
        results = self._table.get_item(
            Key={'identifier_stem': str(table_identifier_stem), 'sid_value': table_identifier_stem.for_dynamo})
        try:
            return results['Item']['column_names'][0]
        except KeyError:
            raise MissingObjectException

    def mark_propagated_vertexes(self, propagation_id, identifier_stem, driving_identifier_stem, driving_id_values, **kwargs):
        driving_identifier_stem = IdentifierStem.from_raw(driving_identifier_stem)
        driving_pairs = driving_identifier_stem.paired_identifiers
        change_types = kwargs['change_types']
        with self._table.batch_writer() as writer:
            for id_value in driving_id_values:
                for change_category in change_types.categories.values():
                    change_pairs = driving_pairs.copy()
                    change_pairs['id_value'] = id_value
                    change_pairs['category'] = str(change_category)
                    change_identifier_stem = IdentifierStem('propagation', identifier_stem.object_type, change_pairs)
                    change = {
                        'identifier_stem': str(change_identifier_stem),
                        'sid_value': str(propagation_id),
                        'propagation_id': str(propagation_id),
                        'driving_identifier_stem': str(driving_identifier_stem),
                        'extracted_identifier_stem': str(identifier_stem),
                        'driving_id_value': id_value
                    }
                    writer.put_item(Item=change)

    def get_propagated_vertexes(self, propagation_id):
        paginator = boto3.client('dynamodb').get_paginator('query')
        iterator = paginator.paginate(
            TableName=self._table_name,
            KeyConditionExpression='#sid=:sid AND begins_with(#id, :id)',
            ExpressionAttributeNames={
                '#sid': 'sid_value', '#id': 'identifier_stem'
            },
            ExpressionAttributeValues={
                ':sid': {'S': str(propagation_id)},
                ':id': {'S': '#propagation#'}
            }
        )
        for page in iterator:
            for item in page['Items']:
                vertex = {
                    'driving_identifier_stem': item['driving_identifier_stem']['S'],
                    'extracted_identifier_stem': item['extracted_identifier_stem']['S'],
                    'driving_id_value': item['driving_id_value']['S'],
                    'identifier_stem': item['identifier_stem']['S']
                }
                yield vertex

    def delete_propagated_vertex(self, propagation_id, identifier_stem):
        self._table.delete_item(Key={
            'sid_value': str(propagation_id),
            'identifier_stem': str(identifier_stem)
        })

    def get_creep_id_values(self, propagation_id):
        paginator = boto3.client('dynamodb').get_paginator('query')
        iterator = paginator.paginate(
            TableName=self._table_name,
            IndexName=self._creep_id_value_index,
            KeyConditionExpression='#pi=:pi',
            ExpressionAttributeNames={
                '#pi': 'propagation_id'
            },
            ExpressionAttributeValues={
                ':pi': {'S': str(propagation_id)}
            }
        )
        for page in iterator:
            for item in page['Items']:
                yield item['id_value']['S']

    def get_creep_categories(self, propagation_id, id_value):
        paginator = boto3.client('dynamodb').get_paginator('query')
        iterator = paginator.paginate(
            TableName=self._table_name,
            IndexName=self._creep_id_value_index,
            KeyConditionExpression='#pi=:pi AND begins_with(#c, :c)',
            ExpressionAttributeNames={
                '#pi': 'propagation_id',
                '#c': 'creep_identifier'
            },
            ExpressionAttributeValues={
                ':pi': {'S': str(propagation_id)},
                ':c': {'S': f'#{id_value}#'}
            }
        )
        for page in iterator:
            for item in page['Items']:
                yield item['category']

    def get_creep_changes(self, propagation_id, id_value, change_category):
        paginator = boto3.client('dynamodb').get_paginator('query')
        iterator = paginator.paginate(
            TableName=self._table_name,
            IndexName=self._creep_id_value_index,
            KeyConditionExpression='#pi=:pi AND begins_with(#c, :c)',
            ExpressionAttributeNames={
                '#pi': 'propagation_id',
                '#c': 'creep_identifier'
            },
            ExpressionAttributeValues={
                ':pi': {'S': str(propagation_id)},
                ':c': {'S': f'#{id_value}#{change_category}'}
            }
        )
        for page in iterator:
            for item in page['Items']:
                vertex = {
                    'driving_identifier_stem': item['driving_identifier_stem']['S'],
                    'extracted_identifier_stem': item['extracted_identifier_stem']['S'],
                    'local_max_value': item['identifier_stem']['N'],
                    'remote_change': json.loads(item['identifier_stem']['S'], cls=AlgDecoder)
                }
                yield vertex

    def get_creep_vertexes(self, propagation_id, change_category, id_value):
        paginator = boto3.client('dynamodb').get_paginator('query')
        iterator = paginator.paginate(
            TableName=self._table_name,
            KeyConditionExpression='#sid=:sid AND begins_with(#id, :id) AND ',
            ExpressionAttributeNames={
                '#sid': 'sid_value', '#id': 'identifier_stem'
            },
            ExpressionAttributeValues={
                ':sid': {'S': str(propagation_id)},
                ':id': {'S': '#creep#ChangeLog#{"category": "' + change_category + '", "id_value": "' + str(id_value)}
            }
        )
        for page in iterator:
            for item in page['Items']:
                vertex = {
                    'driving_identifier_stem': item['driving_identifier_stem']['S'],
                    'extracted_identifier_stem': item['extracted_identifier_stem']['S'],
                    'remote_change': json.loads(item['remote_change']['S'], cls=AlgDecoder),
                    'local_max_value': item['local_max_value']['S']
                }
                yield vertex

    def get_extractor_setup(self, identifier_stem, include_field_values=False):
        setup = {}
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        params = DynamoParameters(identifier_stem.for_dynamo, identifier_stem)
        results = self._table.get_item(
            Key=params.as_key
        )
        try:
            extractor_function_names = results['Item']['extractor_function_names']
            setup.update(extractor_function_names)
        except KeyError:
            raise MissingExtractionInformation(
                'could not find extractor information for identifier stem %s' % identifier_stem)
        if include_field_values:
            try:
                field_values = results['Item']['field_values']
                setup['field_values'] = field_values
            except KeyError:
                raise MissingFieldValuesException(
                    'could not find field values for identifier stem %s' % identifier_stem)
        return setup

    def scan_index_value_max(self, identifier_match, index_name=None):
        if not index_name:
            index_name = self._credible_change_index
        index_values = []
        paginator = boto3.client('dynamodb').get_paginator('scan')
        pages = paginator.paginate(
            TableName=self._table_name,
            IndexName=index_name,
            FilterExpression='begins_with(change_stem, :stem)',
            ExpressionAttributeValues={
                ':stem': {'S': identifier_match}
            }
        )
        for page in pages:
            for item in page['Items']:
                sid_value = item['sid_value']['S']
                sid_value = Decimal(sid_value)
                index_values.append(sid_value)
        if not index_values:
            raise EmptyIndexException
        return max(index_values)

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

    def get_local_id_values(self, identifier_stem, index_name=None, vertex_regulator=None):
        paginator = boto3.client('dynamodb').get_paginator('query')
        if not index_name:
            index_name = self._id_value_index
        query_args = {
            'TableName': self._table_name,
            'IndexName': index_name,
            'KeyConditionExpression': 'identifier_stem = :id',
            'ExpressionAttributeValues': {':id': {'S': str(identifier_stem)}}
        }
        pages = paginator.paginate(**query_args)
        results = set()
        if vertex_regulator:
            results = {'linked': set(), 'unlinked': set(), 'all': set()}
        for page in pages:
            items = page['Items']
            total = len(items)
            progress = 0
            for item in items:
                id_value = item['id_value']['N']
                if vertex_regulator:
                    object_data = identifier_stem.for_extractor
                    object_data['id_value'] = id_value
                    id_source = identifier_stem.get('id_source')
                    potential_vertex = vertex_regulator.create_potential_vertex(object_data)
                    is_linked = self.check_if_id_value_linked(id_source, potential_vertex.internal_id)
                    if is_linked:
                        results['linked'].add(id_value)
                    else:
                        results['unlinked'].add(id_value)
                    results['all'].add(id_value)
                    progress += 1
                    logging.debug(f'{progress}/{total}')
                    continue
                results.add(id_value)
        return results

    def get_local_field_value_keys_max(self, identifier_stem, index_name=None):
        if not index_name:
            index_name = self._field_value_index
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
            raise EmptyIndexException()

    def get_local_field_value_keys(self, identifier_stem, index_name=None):
        paginator = boto3.client('dynamodb').get_paginator('query')
        if not index_name:
            index_name = self._field_value_index
        query_args = {
            'TableName': self._table_name,
            'IndexName': index_name,
            'KeyConditionExpression': Key('identifier_stem').eq(str(identifier_stem)),
            'AttributesToGet': ['id_value']
        }
        results = paginator.paginate(**query_args)
        return results

    def find_potential_vertexes(self, object_type, vertex_properties):
        potential_vertexes, token = self._scan_vertexes(object_type, vertex_properties)
        while token:
            more_vertexes, token = self._scan_vertexes(object_type, vertex_properties, token)
            potential_vertexes.extend(more_vertexes)
        logging.info('completed a scan of the data space to find potential vertexes with properties: %s '
                     'returned the raw values of: %s' % (vertex_properties, potential_vertexes))
        return [PotentialVertex.from_json(x) for x in potential_vertexes]

    def get_field_value_setup(self, identifier_stem):
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        params = DynamoParameters(identifier_stem.for_dynamo, identifier_stem)
        results = self._table.get_item(
            Key=params.as_key
        )
        try:
            field_values = results['Item']['field_values']
        except KeyError:
            raise MissingFieldValuesException(
                'could not find field values for identifier stem %s' % identifier_stem)
        try:
            extractor_function_names = results['Item']['extractor_function_names']
        except KeyError:
            raise MissingExtractionInformation(
                'could not find extractor names for identifier stem %s' % identifier_stem
            )
        return {'field_values': field_values, 'extractor_names': extractor_function_names}

    def get_changelog_types(self, **kwargs):
        index_name = kwargs.get('index_name', 'change_types')
        changelog_types = {}
        paginator = boto3.client('dynamodb').get_paginator('scan')
        comparison_stem = '#vertex#ChangeLogType#'
        filter_category = kwargs.get('category', None)
        if filter_category:
            comparison_stem = '#vertex#ChangeLogType#{"category": "%s' % filter_category
        pages = paginator.paginate(
            TableName=self._table_name,
            IndexName=index_name,
            FilterExpression='begins_with(identifier_stem, :stem)',
            ExpressionAttributeValues={
                ':stem': {'S': comparison_stem}
            }
        )
        for page in pages:
            for item in page['Items']:
                identifier_stem = IdentifierStem.from_raw(item['identifier_stem']['S'])
                category = identifier_stem.get('category')
                if category not in changelog_types:
                    changelog_types[category] = []
                changelog_types[category].append(identifier_stem)
        if kwargs.get('categories_only', False):
            return [x for x in changelog_types.keys()]
        if kwargs.get('category_ids_only', False):
            identifier_stems = []
            for entry in changelog_types.values():
                identifier_stems.extend(entry)
            return [x.get('category_id') for x in identifier_stems]
        if filter_category:
            return changelog_types[filter_category]
        return changelog_types

    def check_if_id_value_linked(self, id_source, internal_id, index_name=None):
        if not index_name:
            index_name = self._links_index
        identified_pairs = {'linked_id_source': id_source, 'internal_id': internal_id}
        identifier_stem = IdentifierStem('vertex', 'link', identified_pairs)
        query_args = {
            'Limit': 1,
            'ScanIndexForward': False,
            'KeyConditionExpression': Key('identifier_stem').eq(str(identifier_stem)),
            'TableName': self._table_name,
            'IndexName': index_name
        }
        results = self._table.query(**query_args)
        try:
            return results['Items'][0]['is_unlink'] is False
        except IndexError:
            raise MissingObjectException()

    def mark_fruiting_complete(self, propagation_id, fruited_id_value, fruited_category):
        self._table.update_item(
            Key={'sid_value': propagation_id, 'identifier_stem': 'propagation'},
            UpdateExpression='SET #did.#id.#ct = :id',
            ExpressionAttributeNames={'#did': 'driving_id_values', '#id': str(fruited_id_value), '#ct': fruited_category},
            ExpressionAttributeValues={':id': 'worked'}
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
