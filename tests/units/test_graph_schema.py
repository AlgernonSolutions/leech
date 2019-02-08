import json

import pytest

from toll_booth.alg_obj.graph.schemata.schema import Schema
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEntry
from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder


@pytest.mark.graph_schema
class TestGraphSchema:
    def test_graph_schema_generation(self):
        schema = Schema.retrieve()
        assert isinstance(schema, Schema)

    def test_graph_schema_entry_generation(self):
        object_type = 'ExternalId'
        schema_entry = SchemaEntry.retrieve(object_type)
        assert isinstance(schema_entry, SchemaEntry)

    def test_json_serialization(self):
        schema = Schema.retrieve()
        schema_str = json.dumps(schema, cls=AlgEncoder)
        rebuilt_schema = json.loads(schema_str, cls=AlgDecoder)
        assert rebuilt_schema == schema
