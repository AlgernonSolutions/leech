import json
import os

from jsonref import JsonRef
from jsonschema import validate

from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry, SchemaEdgeEntry


def get_schema_entry(object_type, **kwargs):
    test_schema = _get_test_schema(**kwargs)
    vertex_entries = test_schema['vertex']
    edge_entries = test_schema['edge']
    for vertex_entry in vertex_entries:
        if vertex_entry['vertex_name'] == object_type:
            return SchemaVertexEntry.parse_json(vertex_entry)
    for edge_entry in edge_entries:
        if edge_entry['edge_label'] == object_type:
            return SchemaEdgeEntry.parse(edge_entry)
    else:
        raise RuntimeError(f'could not find a valid schema entry for {object_type}')


def _get_test_schema(schema_name='schema.json'):
    test_file_name = os.path.dirname(__file__)
    test_schema_file_path = os.path.join(test_file_name, 'schemas', schema_name)
    master_schema_file_path = os.path.join(test_file_name, 'schemas', 'master_schema.json')
    with open(test_schema_file_path) as test, open(master_schema_file_path) as master:
        test_schema = json.load(test)
        test_schema = JsonRef.replace_refs(test_schema)
        master_schema = json.load(master)
        validate(test_schema, master_schema)
        return test_schema
