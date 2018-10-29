from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.aws.aws_obj.sapper import SchemaWhisperer
from toll_booth.alg_obj.graph.schemata.entry_property import SchemaPropertyEntry, EdgePropertyEntry
from toll_booth.alg_obj.graph.schemata.indexes import SortedSetIndexEntry, UniqueIndexEntry
from toll_booth.alg_obj.graph.schemata.rules import VertexRules


class SchemaEntry(AlgObject):
    def __init__(self, entry_name, internal_id_key, entry_properties, indexes, rules):
        self._entry_name = entry_name
        self._internal_id_key = internal_id_key
        self._entry_properties = entry_properties
        self._indexes = indexes
        self._rules = rules

    @classmethod
    def parse(cls, entry_value):
        raise NotImplementedError()

    @classmethod
    def parse_json(cls, json_dict):
        return cls.parse(json_dict)

    @classmethod
    def get(cls, entry_name):
        whisperer = SchemaWhisperer()
        entry = whisperer.get_schema_entry(entry_name)
        entry_value = entry['Items'][0]
        if 'vertex_name' in entry_value:
            return SchemaVertexEntry.parse(entry_value)
        if 'edge_label' in entry_value:
            return SchemaEdgeEntry.parse(entry_value)
        return cls.parse(entry_value)

    @classmethod
    def get_for_index(cls, index_name):
        pass

    @property
    def entry_name(self):
        return self._entry_name

    @property
    def internal_id_key(self):
        return self._internal_id_key

    @property
    def entry_properties(self):
        return self._entry_properties

    @property
    def indexes(self):
        return self._indexes

    @property
    def rules(self):
        return self._rules


class SchemaVertexEntry(SchemaEntry):
    def __init__(self, vertex_name, vertex_properties, internal_id_key, identifier_stem, indexes, rules, extract):
        super().__init__(vertex_name, internal_id_key, vertex_properties, indexes, rules)
        self._vertex_properties = vertex_properties
        self._vertex_name = vertex_name
        self._extract = extract
        self._identifier_stem = identifier_stem

    @classmethod
    def parse(cls, vertex_dict):
        vertex_properties = {}
        indexes = {}
        extraction = {}
        vertex_property_list = vertex_dict['vertex_properties']
        try:
            for entry in vertex_property_list:
                schema_property = SchemaPropertyEntry.parse(entry)
                vertex_properties[schema_property.property_name] = schema_property
        except TypeError:
            vertex_properties = vertex_property_list
        internal_id_key = vertex_dict['internal_id_key']
        identifier_stem = vertex_dict['identifier_stem']
        try:
            internal_id_key = SchemaInternalIdKey(internal_id_key)
        except TypeError:
            internal_id_key = internal_id_key
        try:
            for index_entry in vertex_dict['indexes']:
                try:
                    index = SortedSetIndexEntry.parse(index_entry)
                except KeyError:
                    index = UniqueIndexEntry.parse(index_entry)
                indexes[index.index_name] = index
        except TypeError:
            indexes = vertex_dict['indexes']
        try:
            rules = VertexRules.parse(vertex_dict['rules'])
        except TypeError as e:
            rules = vertex_dict['rules']
        try:
            for extract_entry in vertex_dict['extract']:
                extraction_instructions = ExtractionInstruction.parse(extract_entry)
                extraction[extraction_instructions.extraction_source] = extraction_instructions
        except TypeError:
            extraction = vertex_dict['extract']
        return cls(
            vertex_dict['vertex_name'], vertex_properties, internal_id_key, identifier_stem, indexes, rules, extraction)

    @property
    def vertex_name(self):
        return self.entry_name

    @property
    def object_type(self):
        return self._vertex_name

    @property
    def vertex_properties(self):
        return self.entry_properties

    @property
    def identifier_stem(self):
        return self._identifier_stem

    @property
    def id_value_field(self):
        for vertex_property in self.vertex_properties.values():
            if vertex_property.is_id_value:
                return vertex_property.property_name
        raise NotImplementedError('could not identify id_value field in the schema for vertex: %s' % self._vertex_name)

    @property
    def extraction(self):
        return self._extract

    @property
    def extract(self):
        return self._extract


class SchemaEdgeEntry(SchemaEntry):
    def __init__(self, edge_label, from_type, to_type, edge_properties, internal_id_key, indexes):
        super().__init__(edge_label, internal_id_key, edge_properties, indexes, [])
        self._from_type = from_type
        self._to_type = to_type

    @classmethod
    def parse(cls, edge_dict):
        edge_properties = {}
        indexes = {}
        internal_id_key = SchemaInternalIdKey(edge_dict['internal_id_key'])

        edge_property_list = edge_dict['edge_properties']
        try:
            for entry in edge_property_list:
                schema_property = EdgePropertyEntry.parse(entry)
                edge_properties[schema_property.property_name] = schema_property
        except TypeError:
            edge_properties = edge_property_list
        try:
            for index_entry in edge_dict['indexes']:
                try:
                    index = SortedSetIndexEntry.parse(index_entry)
                except KeyError:
                    index = UniqueIndexEntry.parse(index_entry)
                indexes[index.index_name] = index
        except TypeError:
            indexes = edge_dict['indexes']
        return cls(
            edge_dict['edge_label'], edge_dict['from'], edge_dict['to'], edge_properties, internal_id_key, indexes
        )

    @property
    def edge_label(self):
        return self.entry_name

    @property
    def object_type(self):
        return self._entry_name

    @property
    def edge_properties(self):
        return self.entry_properties

    @property
    def from_type(self):
        return self._from_type

    @property
    def to_type(self):
        return self._to_type


class SchemaInternalIdKey(AlgObject):
    def __init__(self, field_names):
        if isinstance(field_names, SchemaInternalIdKey):
            raise TypeError
        self._field_names = field_names

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['field_names'])

    def __iter__(self):
        return iter(self._field_names)


class SchemaIdentifierStem(AlgObject):
    def __init__(self, field_names):
        if isinstance(field_names, SchemaIdentifierStem):
            raise TypeError
        self._field_names = field_names

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['field_names'])

    def __iter__(self):
        return iter(self._field_names)


class ExtractionInstruction(AlgObject):
    def __init__(self, extraction_source, extraction_properties):
        self._extraction_source = extraction_source
        self._extraction_properties = extraction_properties

    @classmethod
    def parse(cls, extract_dict):
        return cls(extract_dict['extraction_source'], extract_dict['extraction_properties'])

    @classmethod
    def parse_json(cls, json_dict):
        return cls.parse(json_dict)

    @property
    def extraction_source(self):
        return self._extraction_source

    @property
    def extraction_properties(self):
        return self._extraction_properties
