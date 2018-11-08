from tests.units.test_data.schema_generator import get_schema_entry
from tests.units.test_data.vertex_generator import generate_potential_vertex


def generate_transform_results(source_vertex, has_potentials=False):
    potentials = []
    if has_potentials:
        schema_entry = get_schema_entry(source_vertex.object_type)
        schema_rules = schema_entry.rules
        for linking_rules in schema_rules.linking_rules:
            for rule in linking_rules.rules:
                complete_potential_vertex = generate_potential_vertex(rule.target_type)
                potentials.append((complete_potential_vertex, rule))
    return source_vertex, potentials
