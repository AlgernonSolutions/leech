from tests.units.test_data.data_setup.schema_setup.setup import get_test
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry, SchemaEdgeEntry


class MockVertexSchemaEntry:
    @classmethod
    def full_get(cls, object_type, as_dict=False):
        schema = get_test()
        vertex_entries = schema['vertex']
        for vertex_entry in vertex_entries:
            if vertex_entry['vertex_name'] == object_type:
                if as_dict:
                    return vertex_entry
                return SchemaVertexEntry.parse_json(vertex_entry)
        else:
            raise RuntimeError(f'could not find a valid schema entry for {object_type}')

    @classmethod
    def get(cls, context, override_type=None, as_dict=False):
        trial_data = context.active_params
        object_type = trial_data['object_type']
        if override_type:
            object_type = override_type
        vertex_entries = context.schema['vertex']
        for vertex_entry in vertex_entries:
            if vertex_entry['vertex_name'] == object_type:
                if as_dict:
                    return vertex_entry
                return SchemaVertexEntry.parse_json(vertex_entry)
        else:
            raise RuntimeError(f'could not find a valid schema entry for {object_type}')


class MockEdgeSchemaEntry:
    @classmethod
    def from_json(cls, json_dict):
        return SchemaEdgeEntry.parse_json(json_dict)

    @classmethod
    def full_get(cls, object_type, as_dict=False):
        schema = get_test()
        edge_entries = schema['edge']
        for edge_entry in edge_entries:
            if edge_entry['edge_label'] == object_type:
                if as_dict:
                    return edge_entry
                return SchemaEdgeEntry.parse_json(edge_entry)
        else:
            raise RuntimeError()
