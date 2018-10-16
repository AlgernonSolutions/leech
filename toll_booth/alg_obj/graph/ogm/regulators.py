from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.aws.aws_obj.sapper import SecretWhisperer
from toll_booth.alg_obj.graph import InternalId


class ObjectRegulator(AlgObject):
    def __init__(self, schema_entry):
        self._schema_entry = schema_entry
        self._internal_id_key = schema_entry.internal_id_key
        self._entry_properties_schema = schema_entry.entry_properties

    @classmethod
    def get_for_object_type(cls, object_type, rule_entry=None):
        from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEntry
        target_schema_entry = SchemaEntry.get(object_type)
        try:
            getattr(target_schema_entry, 'edge_label')
            return EdgeRegulator(target_schema_entry)
        except AttributeError:
            if not rule_entry:
                # TODO specify this error
                raise RuntimeError('can not instantiate a vertex regulator without a valid rule entry')
            return VertexRegulator(target_schema_entry, rule_entry)

    @classmethod
    def parse_json(cls, json_dict):
        try:
            rule_entry = json_dict['rule_entry']
        except KeyError:
            rule_entry = None
        return cls.get_for_object_type(json_dict['object_type'], rule_entry)

    @property
    def schema_entry(self):
        return self._schema_entry

    def standardize_object_properties(self, graph_object, rule_entry=None, internal_id=None):
        returned_properties = {}
        if not internal_id:
            internal_id = self.create_internal_id(graph_object)
        for property_name, entry_property in self._entry_properties_schema.items():
            try:
                test_property = graph_object[property_name]
            except KeyError:
                returned_properties[property_name] = None
                if not rule_entry or not rule_entry.is_stub:
                    raise RuntimeError('unable to derive object properties for ruled object: %s' % graph_object)
                continue
            test_property, data_type = self._set_property_data_type(property_name, entry_property, test_property)
            if entry_property.sensitive:
                test_property = SecretWhisperer.put_secret(test_property, internal_id, data_type)
            returned_properties[property_name] = test_property
        return returned_properties

    def create_internal_id(self, graph_object):
        try:
            key_values = []
            internal_id_key = self._schema_entry.internal_id_key
            for field_name in internal_id_key:
                key_value = graph_object[field_name]
                key_values.append(str(key_value))
            id_string = ''.join(key_values)
            internal_id = InternalId(id_string).id_value
            return internal_id
        except KeyError:
            return self._internal_id_key

    @classmethod
    def _set_property_data_type(cls, property_name, entry_property, test_property):
        property_data_type = entry_property.property_data_type
        if property_data_type == 'Integer':
            return int(test_property), 'Integer'
        if property_data_type == 'Float':
            return float(test_property), 'Float'
        if property_data_type == 'String':
            return str(test_property), 'String'
        if property_data_type == 'DateTime':
            from toll_booth.alg_obj.utils import convert_credible_datetime_to_gremlin
            is_utc = 'utc' in property_name
            return convert_credible_datetime_to_gremlin(test_property, is_utc), 'DateTime'


class VertexRegulator(ObjectRegulator):
    def __init__(self, schema_entry, rule_entry):
        super().__init__(schema_entry)
        self._rule_entry = rule_entry

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['schema_entry'], json_dict['rule_entry'])

    def standardize_object_properties(self, graph_object, **kwargs):
        return super().standardize_object_properties(graph_object, self._rule_entry)


class EdgeRegulator(ObjectRegulator):
    def standardize_edge_properties(self, edge_properties, source_vertex, potential_other, inbound):
        returned_properties = super().standardize_object_properties(edge_properties)
        accepted_source_vertex = self._schema_entry.from_type
        accepted_target_vertex = self._schema_entry.to_type
        source_object_type = source_vertex.object_type
        potential_other_type = potential_other.object_type
        try:
            self.validate_edge_origins(accepted_source_vertex, source_object_type, potential_other_type, inbound)
            self.validate_edge_origins(accepted_target_vertex, potential_other_type, source_object_type, inbound)
        except RuntimeError:
            raise RuntimeError(
                'error trying to build edge between %s and %s, '
                'schema constraint fails, accepted vertexes: %s/%s' % (
                    source_vertex, potential_other, accepted_source_vertex, accepted_target_vertex
                )
            )
        return returned_properties

    @staticmethod
    def validate_edge_origins(accepted_vertex_types, test_vertex, other_vertex, inbound):
        if accepted_vertex_types == '*':
            return
        if inbound:
            if accepted_vertex_types == other_vertex:
                return
        if accepted_vertex_types == test_vertex:
            return
        raise RuntimeError()

    def generate_potential_edge(self, source_vertex, potential_other, extracted_data, inbound):
        edge_label = self._schema_entry.edge_label
        edge_properties = self.generate_edge_properties(
            source_vertex, potential_other, extracted_data, inbound)
        edge_properties = self.standardize_edge_properties(
            edge_properties, source_vertex, potential_other, inbound)
        try:
            edge_internal_id = self.create_edge_internal_id(
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
        edge_properties = self.generate_edge_properties(
            source_vertex, stubbed_other, extracted_data, inbound, True
        )
        source_vertex_internal_id = source_vertex.internal_id
        stub_properties = stubbed_other.vertex_properties
        if inbound:
            return PotentialEdge(edge_label, None, edge_properties, stub_properties, source_vertex_internal_id)
        return PotentialEdge(edge_label, None, edge_properties, source_vertex_internal_id, stub_properties)

    def create_edge_internal_id(self, inbound, **kwargs):
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

    def generate_edge_properties(self, source_vertex, ruled_target, extracted_data, inbound, for_stub=False):
        edge_properties = {}
        for edge_property_name, edge_property in self._entry_properties_schema.items():
            try:
                edge_value = self._generate_edge_property(
                    edge_property_name, edge_property, source_vertex, ruled_target, extracted_data, inbound)
            except KeyError:
                if for_stub:
                    edge_value = None
                else:
                    raise RuntimeError(
                        'could not derive value for edge property: %s, %s' % edge_property_name, edge_property)
            edge_properties[edge_property_name] = edge_value
        return edge_properties

    def _generate_edge_property(self, edge_property_name, property_schema, source_vertex, other_vertex, extracted_data,
                                inbound):
        property_source = property_schema.property_source
        source_type = property_source['source_type']
        if source_type == 'source_vertex':
            return self.generate_vertex_held_property(property_source, source_vertex, other_vertex, inbound)
        if source_type == 'target_vertex':
            return self.generate_vertex_held_property(property_source, other_vertex, source_vertex, inbound)
        if source_type == 'extraction':
            return self.derive_extracted_property(
                edge_property_name, property_source['extraction_name'], extracted_data)
        if source_type == 'function':
            return self.execute_property_function(
                property_source['function_name'], source_vertex, other_vertex, extracted_data, self._schema_entry,
                inbound
            )
        raise NotImplementedError('edge property source: %s is not registered with the system' % source_type)

    @staticmethod
    def generate_vertex_held_property(property_source, holding_vertex, other_vertex, inbound):
        vertex_property_name = property_source['vertex_property_name']
        if inbound:
            return other_vertex[vertex_property_name]
        return holding_vertex[vertex_property_name]

    @staticmethod
    def derive_extracted_property(property_name, extraction_name, extracted_data):
        potential_properties = set()
        target_extraction = extracted_data[extraction_name]
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
    def execute_property_function(function_name, source_vertex, ruled_target, extracted_data, schema_entry, inbound):
        from toll_booth.alg_obj.forge import specifiers
        try:
            specifier_function = getattr(specifiers, function_name)
        except AttributeError:
            raise NotImplementedError('specifier function named: %s is not registered with the system' % function_name)
        return specifier_function(
            source_vertex=source_vertex, ruled_target=ruled_target, extracted_data=extracted_data,
            schema_entry=schema_entry, inbound=inbound)


class GraphObject(AlgObject):
    def __init__(self, object_type, object_properties):
        self._object_type = object_type
        self._object_properties = object_properties
        self._graph_as_stub = False

    @property
    def object_type(self):
        return self._object_type

    @property
    def object_properties(self):
        return self._object_properties

    def __getitem__(self, item):
        try:
            return getattr(self, item)
        except AttributeError:
            return self._object_properties[item]


class PotentialVertex(GraphObject):
    def __init__(self, object_type, internal_id, object_properties, is_stub):
        super().__init__(object_type, object_properties)
        self._internal_id = internal_id
        self._is_stub = is_stub

    @classmethod
    def for_known_vertex(cls, object_data, schema_entry):
        regulator = ObjectRegulator(schema_entry)
        object_type = schema_entry.vertex_name
        internal_id = regulator.create_internal_id(object_data)
        object_properties = regulator.standardize_object_properties(object_data)
        return cls(object_type, internal_id, object_properties, False)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['object_type'], json_dict['internal_id'], json_dict['object_properties'], json_dict['is_stub'])

    @property
    def internal_id(self):
        return self._internal_id

    @property
    def is_identifiable(self):
        if not isinstance(self._internal_id, str):
            return False
        return True

    @property
    def is_stub(self):
        return self._is_stub

    @property
    def graphed_object_type(self):
        if self.is_stub:
            return f'{self.object_type}::stub'
        return self.object_type


class PotentialEdge(GraphObject):
    def __init__(self, edge_label, internal_id, edge_properties, from_object, to_object):
        super().__init__(edge_label, edge_properties)
        self._internal_id = internal_id
        self._from_object = from_object
        self._to_object = to_object

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['edge_label'], json_dict['internal_id'],
            json_dict['edge_properties'], json_dict['from_object'], json_dict['to_object']
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
    def internal_id(self):
        return self._internal_id

    @property
    def from_object(self):
        return self._from_object

    @property
    def to_object(self):
        return self._to_object

    @property
    def to_json(self):
        return {
            'edge_label': self.edge_label,
            'internal_id': self._internal_id,
            'edge_properties': self.edge_properties,
            'from_object': self._from_object,
            'to_object': self._to_object
        }


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
        results = table.quer(
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
