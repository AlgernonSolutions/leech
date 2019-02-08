import json

import jsonref
from jsonschema import validate

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.aws.snakes.schema_snek import SchemaSnek
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry, SchemaEdgeEntry


class Schema(AlgObject):
    def __init__(self, vertex_entries, edge_entries):
        self._vertex_entries = {
            x['vertex_name']: SchemaVertexEntry.parse(x) for x in vertex_entries
        }
        self._edge_entries = {
            x['edge_label']: SchemaEdgeEntry.parse(x) for x in edge_entries
        }

    @property
    def vertex_entries(self):
        return self._vertex_entries

    @property
    def edge_entries(self):
        return self._edge_entries

    def __getitem__(self, item):
        try:
            return SchemaVertexEntry.parse(self._vertex_entries[item])
        except KeyError:
            return SchemaEdgeEntry.parse(self._edge_entries[item])

    @classmethod
    def retrieve(cls, **kwargs):
        schema_writer = SchemaSnek(**kwargs)
        current_entries = schema_writer.get_schema()
        return cls(current_entries['vertex'], current_entries['edge'])

    @classmethod
    def post(cls, schema_file_path, validation_schema_file_path, **kwargs):
        schema_snek = SchemaSnek(**kwargs)
        with open(schema_file_path) as schema_file, open(validation_schema_file_path) as validation_file:
            working_schema = jsonref.load(schema_file)
            master_schema = jsonref.load(validation_file)
            validate(working_schema, master_schema)
            schema_snek.put_schema(schema_file_path, **kwargs)
            schema_snek.put_validation_schema(schema_file_path, **kwargs)
            return cls(working_schema['vertex'], working_schema['edge'])

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['vertex_entries'], json_dict['edge_entries'])
