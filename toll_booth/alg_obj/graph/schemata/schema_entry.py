from toll_booth.alg_obj import AlgObject


class SchemaEntry(AlgObject):
    def __init__(self, entry_name, internal_id_key, entry_properties, indexes, rules):
        self._entry_name = entry_name
        self._internal_id_key = internal_id_key
        self._entry_properties = entry_properties
        self._indexes = indexes
        self._rules = rules

    @classmethod
    def parse_json(cls, json_dict):
        raise NotImplementedError

    @classmethod
    def retrieve(cls, entry_name):
        from toll_booth.alg_obj.graph.schemata.schema import Schema
        schema = Schema.retrieve()
        return schema[entry_name]

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
    def parse_json(cls, json_dict):
        return cls(
            json_dict['vertex_name'], json_dict['vertex_properties'], json_dict['internal_id_key'],
            json_dict['identifier_stem'], json_dict['indexes'], json_dict['rules'], json_dict['extract']
        )

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
    def parse_json(cls, json_dict):
        return cls(
            json_dict['entry_name'], json_dict['from_type'], json_dict['to_type'],
            json_dict['entry_properties'], json_dict['internal_id_key'], json_dict['indexes']
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
    def from_types(self):
        return self._from_type

    @property
    def to_types(self):
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

    def __str__(self):
        return '-'.join(self._field_names)


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
    def parse_json(cls, json_dict):
        return cls(json_dict['extraction_source'], json_dict['extraction_properties'])

    @property
    def extraction_source(self):
        return self._extraction_source

    @property
    def extraction_properties(self):
        return self._extraction_properties
