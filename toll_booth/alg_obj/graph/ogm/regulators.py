import json
import re
from collections import OrderedDict
from decimal import Decimal

import dateutil

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.aws.sapper.secrets import SecretWhisperer
from toll_booth.alg_obj.graph import InternalId
from toll_booth.alg_obj.serializers import AlgEncoder


class ObjectRegulator:
    def __init__(self, schema_entry):
        self._schema_entry = schema_entry
        self._internal_id_key = schema_entry.internal_id_key
        self._entry_properties_schema = schema_entry.entry_properties

    @classmethod
    def get_for_schema_entry(cls, schema_entry):
        try:
            getattr(schema_entry, 'edge_label')
            return EdgeRegulator(schema_entry)
        except AttributeError:
            return VertexRegulator(schema_entry)

    @classmethod
    def get_for_object_type(cls, object_type, schema_entry=None):
        from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEntry
        if not schema_entry:
            schema_entry = SchemaEntry.retrieve(object_type)
        try:
            getattr(schema_entry, 'edge_label')
            return EdgeRegulator(schema_entry)
        except AttributeError:
            return VertexRegulator(schema_entry)

    @property
    def schema_entry(self):
        return self._schema_entry

    def _obfuscate_sensitive_data(self, internal_id, graph_object_properties):
        returned_data = {}
        for property_name, entry_property in self._entry_properties_schema.items():
            property_data_type = entry_property.property_data_type
            property_value = graph_object_properties[property_name]
            if property_value and entry_property.sensitive:
                if hasattr(property_value, 'is_missing'):
                    property_value = 'AlgernonSensitiveDataFieldMissingValue'
                    returned_data[property_name] = property_value
                    continue
                if not isinstance(internal_id, str):
                    raise RuntimeError(
                        f'object property named {property_name} is listed as being sensitive, but the parent object '
                        f'could not be uniquely identified. sensitive properties use their parent objects identifier '
                        f'to guarantee uniqueness. object containing sensitive properties generally can not be stubbed'
                    )
                property_value = SecretWhisperer.put_secret(property_value, internal_id, property_data_type)
            returned_data[property_name] = property_value
        return returned_data

    def _standardize_object_properties(self, graph_object):
        returned_properties = {}
        for property_name, entry_property in self._entry_properties_schema.items():
            try:
                test_property = graph_object[property_name]
            except KeyError:
                returned_properties[property_name] = MissingObjectProperty()
                continue
            test_property = self._set_property_data_type(property_name, entry_property, test_property)
            returned_properties[property_name] = test_property
        return returned_properties

    def _create_internal_id(self, graph_object, for_known=False):
        static_key_fields = {
            'object_type': self._schema_entry.entry_name,
            'id_value_field': self._schema_entry.id_value_field
        }
        try:
            key_values = []
            internal_id_key = self._schema_entry.internal_id_key
            for field_name in internal_id_key:
                if field_name in static_key_fields:
                    key_values.append(str(static_key_fields[field_name]))
                    continue
                if hasattr(field_name, 'is_missing'):
                    key_values.append('MISSING_OBJECT_PROPERTY')
                key_value = graph_object[field_name]
                key_values.append(str(key_value))
            id_string = ''.join(key_values)
            internal_id = InternalId(id_string).id_value
            return internal_id
        except KeyError:
            if for_known:
                raise RuntimeError(
                    f'could not calculate internal id for a source/known object, this generally indicates that the '
                    f'extraction for that object was flawed. error for graph object: {graph_object}'
                )
            return self._internal_id_key

    def _create_identifier_stem(self, potential_object, object_data):
        try:
            paired_identifiers = {}

            identifier_stem_key = self._schema_entry.identifier_stem
            object_type = self._schema_entry.object_type
            for field_name in identifier_stem_key:
                try:
                    key_value = potential_object[field_name]
                except KeyError:
                    key_value = object_data[field_name]
                if isinstance(key_value, MissingObjectProperty):
                    return self._schema_entry.identifier_stem
                if key_value is None and '::stub' not in object_type:
                    object_type = object_type + '::stub'
                paired_identifiers[field_name] = key_value
            return IdentifierStem('vertex', object_type, paired_identifiers)
        except KeyError:
            return self._schema_entry.identifier_stem

    def _create_id_value(self, potential_object):
        try:
            id_value = potential_object[self._schema_entry.id_value_field]
            vertex_properties = self._schema_entry.vertex_properties
            id_value_properties = vertex_properties[self._schema_entry.id_value_field]
            if id_value_properties.property_data_type == 'DateTime':
                remade_date_value = dateutil.parser.parse(id_value)
                id_value = Decimal(remade_date_value.timestamp())
            return id_value
        except KeyError:
            return self._schema_entry.id_value_field

    @classmethod
    def _set_property_data_type(cls, property_name, entry_property, test_property):
        property_data_type = entry_property.property_data_type
        if not test_property:
            return None
        if test_property == '':
            return None
        if property_data_type == 'Number':
            try:
                return Decimal(test_property)
            except TypeError:
                return Decimal(test_property.timestamp())
        if property_data_type == 'String':
            return str(test_property)
        if property_data_type == 'DateTime':
            from toll_booth.alg_obj.utils import convert_python_datetime_to_gremlin
            return convert_python_datetime_to_gremlin(test_property)
        raise NotImplementedError(
            f'data type {property_data_type} for property named: {property_name} is unknown to the system')


class VertexRegulator(ObjectRegulator):
    def __init__(self, schema_entry):
        super().__init__(schema_entry)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['schema_entry'])

    def create_potential_vertex(self, object_data, **kwargs):
        if 'object_properties' in object_data:
            raise RuntimeError('trying to build a vertex from the wrong level, you must create potential vertexes '
                               'from their object_properties, passing in internal_id, identifier_stem, id_value as '
                               'kwargs if needed')
        object_properties = self._standardize_object_properties(object_data)
        internal_id = kwargs.get('internal_id', self._create_internal_id(object_properties))
        identifier_stem = kwargs.get('identifier_stem', self._create_identifier_stem(object_properties, object_data))
        id_value = kwargs.get('id_value', self._create_id_value(object_properties))
        object_properties = self._obfuscate_sensitive_data(internal_id, object_properties)
        object_type = self._schema_entry.object_type
        id_value_field = self._schema_entry.id_value_field
        return PotentialVertex(
           object_type, internal_id, object_properties, identifier_stem, id_value, id_value_field)


class EdgeRegulator(ObjectRegulator):
    def standardize_edge_properties(self, edge_properties, source_vertex, potential_other, inbound):
        returned_properties = super()._standardize_object_properties(edge_properties)
        accepted_source_vertexes = self._schema_entry.from_types
        accepted_target_vertexes = self._schema_entry.to_types
        source_object_type = source_vertex.object_type
        potential_other_type = potential_other.object_type
        try:
            self.validate_edge_origins(accepted_source_vertexes, source_object_type, potential_other_type, inbound)
            self.validate_edge_origins(accepted_target_vertexes, potential_other_type, source_object_type, inbound)
        except RuntimeError:
            raise RuntimeError(
                f'error trying to build a {self._schema_entry.edge_label} edge between '
                f'{source_vertex} and {potential_other}, '
                f'schema constraint fails, accepted vertexes: {accepted_source_vertexes}/{accepted_target_vertexes}'
            )
        return returned_properties

    def validate_edge_origins(self, accepted_vertex_types, test_vertex, other_vertex, inbound):
        if '*' in accepted_vertex_types:
            return
        if inbound:
            if other_vertex in accepted_vertex_types:
                return
        if test_vertex in accepted_vertex_types:
            return
        raise RuntimeError(f'attempted to create a {self._schema_entry.edge_label} edge '
                           f'between {test_vertex} and {other_vertex}, '
                           f'but failed constraint for edge origins, accepted types: {accepted_vertex_types}')

    def generate_potential_edge(self, source_vertex, potential_other, extracted_data, inbound):
        edge_label = self._schema_entry.edge_label
        edge_properties = self._generate_edge_properties(
            source_vertex, potential_other, extracted_data, inbound)
        edge_properties = self.standardize_edge_properties(
            edge_properties, source_vertex, potential_other, inbound)
        try:
            edge_internal_id = self._create_edge_internal_id(
                inbound, source_vertex=source_vertex, potential_other=potential_other, edge_properties=edge_properties)
        except KeyError:
            edge_internal_id = self._schema_entry.internal_id_key_fields
        source_internal_id = source_vertex.internal_id
        potential_other_id = potential_other.internal_id
        if inbound:
            return PotentialEdge(edge_label, edge_internal_id, edge_properties, potential_other_id, source_internal_id)
        return PotentialEdge(edge_label, edge_internal_id, edge_properties, source_internal_id, potential_other_id)

    def generate_stubbed_edge(self, source_vertex, stubbed_other, extracted_data, inbound):
        edge_label = self._schema_entry.edge_label
        edge_properties = self._generate_edge_properties(
            source_vertex, stubbed_other, extracted_data, inbound, True
        )
        source_vertex_internal_id = source_vertex.internal_id
        stub_properties = stubbed_other.vertex_properties
        if inbound:
            return PotentialEdge(edge_label, None, edge_properties, stub_properties, source_vertex_internal_id)
        return PotentialEdge(edge_label, None, edge_properties, source_vertex_internal_id, stub_properties)

    def _create_edge_internal_id(self, inbound, **kwargs):
        key_values = []
        for key_field in self._schema_entry.internal_id_key:
            if 'to.' in key_field:
                source_key_field = key_field.replace('to.', '')
                source_name = 'potential_other'
                if inbound:
                    source_name = 'source_vertex'
                key_value = kwargs[source_name][source_key_field]
                key_values.append(key_value)
                continue
            if 'from.' in key_field:
                source_key_field = key_field.replace('from.', '')
                source_name = 'source_vertex'
                if inbound:
                    source_name = 'potential_other'
                key_value = kwargs[source_name][source_key_field]
                key_values.append(key_value)
                continue
            if 'schema.' in key_field:
                key_field = key_field.replace('schema.', '')
                key_value = getattr(self._schema_entry, key_field)
                key_values.append(key_value)
                continue
            key_value = kwargs['edge_properties'][key_field]
            key_values.append(key_value)
        internal_id = InternalId(''.join(key_values))
        return internal_id.id_value

    @staticmethod
    def derive_source_internal_id(**kwargs):
        if kwargs['inbound']:
            return kwargs['ruled_target'].internal_id
        return kwargs['source_vertex'].internal_id

    def _generate_edge_properties(self, source_vertex, ruled_target, extracted_data, inbound, for_stub=False):
        edge_properties = {}
        for edge_property_name, edge_property in self._entry_properties_schema.items():
            try:
                edge_value = self._generate_edge_property(
                    edge_property_name, edge_property, source_vertex=source_vertex,
                    other_vertex=ruled_target, extracted_data=extracted_data, inbound=inbound)
            except KeyError:
                if for_stub:
                    edge_value = None
                else:
                    raise RuntimeError(
                        'could not derive value for edge property: %s, %s' % (edge_property_name, edge_property))
            edge_properties[edge_property_name] = edge_value
        return edge_properties

    def _generate_edge_property(self, edge_property_name, property_schema, **kwargs):
        inbound = kwargs['inbound']
        source_vertex = kwargs['source_vertex']
        other_vertex = kwargs['other_vertex']
        property_source = property_schema.property_source
        source_type = property_source['source_type']
        extracted_data = kwargs['extracted_data']
        if source_type == 'source_vertex':
            return self._generate_vertex_held_property(property_source, source_vertex, other_vertex, inbound)
        if source_type == 'target_vertex':
            return self._generate_vertex_held_property(property_source, other_vertex, source_vertex, inbound)
        if source_type == 'extraction':
            return self._derive_extracted_property(
                edge_property_name, property_source['extraction_name'], extracted_data)
        if source_type == 'function':
            return self._execute_property_function(
                property_source['function_name'], source_vertex, other_vertex, extracted_data, self._schema_entry,
                inbound
            )
        raise NotImplementedError('edge property source: %s is not registered with the system' % source_type)

    @staticmethod
    def _generate_vertex_held_property(property_source, holding_vertex, other_vertex, inbound):
        vertex_property_name = property_source['vertex_property_name']
        if inbound:
            return other_vertex[vertex_property_name]
        return holding_vertex[vertex_property_name]

    @staticmethod
    def _derive_extracted_property(property_name, extraction_name, extracted_data):
        potential_properties = set()
        try:
            target_extraction = extracted_data[extraction_name]
        except KeyError:
            raise RuntimeError(f'during the extraction derivation of an edge property, a KeyError was encountered, '
                               f'extraction data source named: {extraction_name} was not found in the extracted data: '
                               f'{extracted_data}')
        for extraction in target_extraction:
            potential_properties.add(extraction[property_name])
        if len(potential_properties) > 1:
            raise RuntimeError(
                'attempted to derive an edge property from an extraction, but the extraction yielded multiple '
                'potential values, currently only one extracted value per extraction is supported, '
                'property_name: %s, extraction_name: %s, extracted_data: %s' % (
                    property_name, extraction_name, extracted_data)
            )
        for _ in potential_properties:
            return _

    @staticmethod
    def _execute_property_function(function_name, source_vertex, ruled_target, extracted_data, schema_entry, inbound):
        from toll_booth.alg_obj.forge import specifiers
        try:
            specifier_function = getattr(specifiers, function_name)
        except AttributeError:
            raise NotImplementedError('specifier function named: %s is not registered with the system' % function_name)
        return specifier_function(
            source_vertex=source_vertex, ruled_target=ruled_target, extracted_data=extracted_data,
            schema_entry=schema_entry, inbound=inbound)


class GraphObject(AlgObject):
    def __init__(self, object_type, object_properties, internal_id, identifier_stem, id_value, id_value_field):
        self._object_type = object_type
        self._object_properties = object_properties
        self._internal_id = internal_id
        self._identifier_stem = identifier_stem
        self._id_value = id_value
        self._id_value_field = id_value_field
        self._graph_as_stub = False

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['object_type'], json_dict['object_properties'], json_dict['internal_id'],
            json_dict['identifier_stem'], json_dict['id_value'], json_dict['id_value_field']
        )

    @property
    def object_type(self):
        return self._object_type

    @property
    def object_properties(self):
        return self._object_properties

    @property
    def internal_id(self):
        return self._internal_id

    @property
    def identifier_stem(self):
        return self._identifier_stem

    @property
    def id_value(self):
        return self._id_value

    @property
    def id_value_field(self):
        return self._id_value_field

    @property
    def graph_as_stub(self):
        return self._graph_as_stub

    @property
    def for_index(self):
        indexed_value = {
            'sid_value': str(self._id_value),
            'identifier_stem': str(self._identifier_stem),
            'internal_id': str(self._internal_id),
            'id_value': self._id_value,
            'object_type': self._object_type,
            'object_value': json.dumps(self, cls=AlgEncoder),
            'object_properties': self._object_properties
        }
        if isinstance(self._id_value, int) or isinstance(self._id_value, Decimal):
            indexed_value['numeric_id_value'] = self._id_value
        for property_name, property_value in self._object_properties.items():
            indexed_value[property_name] = property_value
        return indexed_value

    @property
    def for_stub_index(self):
        return json.dumps(self, cls=AlgEncoder)

    @property
    def is_edge(self):
        return '#edge#' in str(self._identifier_stem)

    @property
    def is_identifiable(self):
        try:
            identifier_stem = IdentifierStem.from_raw(self._identifier_stem)
        except AttributeError:
            return False
        if not self.is_internal_id_set:
            return False
        if not isinstance(identifier_stem, IdentifierStem):
            return False
        if not self.is_id_value_set:
            return False
        return True

    @property
    def is_identifier_stem_set(self):
        try:
            IdentifierStem.from_raw(self._identifier_stem)
            return True
        except AttributeError:
            return False

    @property
    def is_properties_complete(self):
        for property_name, object_property in self._object_properties.items():
            if hasattr(object_property, 'is_missing'):
                return False
        return True

    @property
    def is_id_value_set(self):
        return self._id_value != self._id_value_field

    @property
    def is_internal_id_set(self):
        return isinstance(self._internal_id, str)

    def __getitem__(self, item):
        try:
            return getattr(self, item)
        except AttributeError:
            return self._object_properties[item]


class PotentialVertex(GraphObject):
    def __init__(self, object_type, internal_id, object_properties, identifier_stem, id_value, id_value_field):
        super().__init__(object_type, object_properties, internal_id, identifier_stem, id_value, id_value_field)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['object_type'], json_dict.get('internal_id'),
            json_dict.get('object_properties', {}), json_dict['identifier_stem'],
            json_dict.get('id_value'), json_dict.get('id_value_field')
        )

    @property
    def graphed_object_type(self):
        return self._identifier_stem.object_type

    def __str__(self):
        return f'{self._object_type}-{self.id_value}'


class PotentialEdge(GraphObject):
    def __init__(self, object_type, internal_id, object_properties, from_object, to_object):
        identifier_stem = IdentifierStem.from_raw(f'#edge#{object_type}#')
        id_value = internal_id
        id_value_field = 'internal_id'
        super().__init__(object_type, object_properties, internal_id, identifier_stem, id_value, id_value_field)
        self._from_object = from_object
        self._to_object = to_object

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['object_type'], json_dict['internal_id'],
            json_dict['object_properties'], json_dict['from_object'], json_dict['to_object']
        )

    @property
    def edge_label(self):
        return self._object_type

    @property
    def graphed_object_type(self):
        return self.edge_label

    @property
    def edge_properties(self):
        return self._object_properties

    @property
    def from_object(self):
        return self._from_object

    @property
    def to_object(self):
        return self._to_object


class SensitiveData:
    def __init__(self, sensitive_entry, data_type, source_internal_id, internal_id=None):
        if not internal_id:
            id_string = ''.join([data_type, source_internal_id])
            internal_id = InternalId(id_string).id_value
        self._sensitive_entry = sensitive_entry
        self._data_type = data_type
        self._source_internal_id = source_internal_id
        self._insensitive = internal_id
        self.update_sensitive()

    @classmethod
    def from_insensitive(cls, insensitive_entry, sensitive_table_name=None):
        import boto3
        from boto3.dynamodb.conditions import Key

        if not sensitive_table_name:
            import os
            sensitive_table_name = os.environ['SENSITIVE_TABLE']
        resource = boto3.resource('dynamodb')
        table = resource.Table(sensitive_table_name)
        results = table.query(
            IndexName='string',
            Select='ALL_ATTRIBUTES',
            KeyConditionExpression=Key('insensitive').eq(insensitive_entry)
        )
        print(results)

    def __str__(self):
        return self._insensitive

    def __get__(self, instance, owner):
        return self._insensitive

    @property
    def sensitive_entry(self):
        return self._sensitive_entry

    def update_sensitive(self, sensitive_table_name=None):
        import boto3
        from botocore.exceptions import ClientError
        if not sensitive_table_name:
            import os
            sensitive_table_name = os.environ['SENSITIVE_TABLE']
        resource = boto3.resource('dynamodb')
        table = resource.Table(sensitive_table_name)
        try:
            table.update_item(
                Key={'insensitive': self._insensitive},
                UpdateExpression='SET sensitive_entry = if_not_exists(sensitive_entry, :s)',
                ExpressionAttributeValues={':s': self._sensitive_entry},
                ReturnValues='NONE'
            )
        except ClientError as e:
            print(e)


class MissingObjectProperty(AlgObject):
    @classmethod
    def is_missing(cls):
        return True

    @classmethod
    def parse_json(cls, json_dict):
        return cls()


class IdentifierStem(AlgObject):
    def __init__(self, graph_type, object_type, paired_identifiers=None):
        if not paired_identifiers:
            paired_identifiers = OrderedDict()
        self._graph_type = graph_type
        self._object_type = object_type
        self._paired_identifiers = paired_identifiers

    @classmethod
    def from_raw(cls, identifier_stem):
        if isinstance(identifier_stem, IdentifierStem):
            return identifier_stem
        pieces = identifier_stem.split('#')
        graph_type = pieces[1]
        object_type = pieces[2]
        paired_identifiers = {}
        pattern = re.compile('({(.*?)})')
        potential_pairs = pattern.search(identifier_stem)
        if potential_pairs:
            paired_identifiers = json.loads(potential_pairs.group(0), object_pairs_hook=OrderedDict)
        return cls(graph_type, object_type, paired_identifiers)

    @classmethod
    def for_stub(cls, stub_vertex):
        identifier_stem = stub_vertex.identifier_stem
        try:
            identifier_stem = IdentifierStem.from_raw(identifier_stem)
            return identifier_stem
        except AttributeError:
            pass
        object_type = getattr(stub_vertex, 'object_type', 'UNKNOWN')
        paired_identifiers = {}
        for property_field in identifier_stem:
            property_value = stub_vertex.object_properties.get(property_field, None)
            if hasattr(property_value, 'is_missing'):
                property_value = None
            paired_identifiers[property_field] = property_value
        if not stub_vertex.is_properties_complete:
            object_type = object_type + '::stub'
        return cls('vertex', object_type, paired_identifiers)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['graph_type'], json_dict['object_type'],
            json_dict.get('paired_identifiers')
        )

    @property
    def object_type(self):
        return self._object_type

    @property
    def paired_identifiers(self):
        return self._paired_identifiers

    @property
    def is_edge(self):
        return self._graph_type == 'edge'

    @property
    def for_dynamo(self):
        return f'#SOURCE{str(self)}'

    @property
    def for_extractor(self):
        extractor_data = self._paired_identifiers.copy()
        extractor_data.update({
            'graph_type': self._graph_type,
            'object_type': self._object_type
        })
        return extractor_data

    @property
    def is_stub(self):
        return '::stub' in self._object_type

    @property
    def as_stub_for_object(self):
        return f'''#{self._graph_type}#{self._object_type}::stub#{self._string_paired_identifiers()}#'''

    def specify(self, identifier_stem, id_value):
        paired_identifiers = self._paired_identifiers.copy()
        paired_identifiers['identifier_stem'] = str(identifier_stem)
        paired_identifiers['id_value'] = int(id_value)
        return IdentifierStem(self._graph_type, self._object_type, paired_identifiers)

    def _string_paired_identifiers(self):
        return json.dumps(self._paired_identifiers)

    def get(self, item):
        if item == 'graph_type':
            return self._graph_type
        if item == 'object_type':
            return self._object_type
        if item in self._paired_identifiers.keys():
            return self._paired_identifiers[item]
        raise AttributeError

    def __getitem__(self, item):
        if item == 'graph_type':
            return self._graph_type
        if item == 'object_type':
            return self._object_type
        if item in self._paired_identifiers:
            return self._paired_identifiers[item]
        raise AttributeError

    def __str__(self):
        return f'''#{self._graph_type}#{self._object_type}#{self._string_paired_identifiers()}#'''
