import jsonref
from jsonschema import validate

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.aws.snakes.schema_snek import SchemaSnek
from toll_booth.alg_obj.graph.schemata.schema_parser import SchemaParer


class Schema(AlgObject):
    def __init__(self, vertex_entries=None, edge_entries=None):
        if not vertex_entries:
            vertex_entries = {}
        if not edge_entries:
            edge_entries = {}
        self._vertex_entries = vertex_entries
        self._edge_entries = edge_entries

    @property
    def vertex_entries(self):
        return self._vertex_entries

    @property
    def edge_entries(self):
        return self._edge_entries

    def __getitem__(self, item):
        try:
            return self._vertex_entries[item]
        except KeyError:
            return self._edge_entries[item]

    def get(self,  item, default=None):
        try:
            return self[item]
        except KeyError:
            return default

    @classmethod
    def retrieve(cls, **kwargs):
        schema_writer = SchemaSnek(**kwargs)
        json_schema = schema_writer.get_schema()
        vertex_entries, edge_entries = SchemaParer.parse(json_schema)
        return cls(vertex_entries, edge_entries)

    @classmethod
    def post(cls, schema_file_path, validation_schema_file_path, **kwargs):
        schema_snek = SchemaSnek(**kwargs)
        with open(schema_file_path) as schema_file, open(validation_schema_file_path) as validation_file:
            working_schema = jsonref.load(schema_file)
            master_schema = jsonref.load(validation_file)
            validate(working_schema, master_schema)
            schema_snek.put_schema(schema_file_path, **kwargs)
            schema_snek.put_validation_schema(schema_file_path, **kwargs)
            vertex_entries = {x.entry_name for x in working_schema['vertex']}
            edge_entries = {x.entry_name for x in working_schema['edge']}
            return cls(vertex_entries, edge_entries)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['vertex_entries'], json_dict['edge_entries'])

    def add_vertex_entry(self, vertex_entry):
        self._vertex_entries[vertex_entry.vertex_name] = vertex_entry

    def add_edge_entry(self, edge_entry):
        self._edge_entries[edge_entry.edge_label] = edge_entry
