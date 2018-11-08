from tests.units.test_data.schema_generator import get_schema_entry
from tests.units.test_data.vertex_generator import generate_potential_vertex, generate_potential_edge


def generate_assimilation_results_set(source_vertex):
    results_sets = []
    schema_entry = get_schema_entry(source_vertex.object_type)
    schema_rules = schema_entry.rules
    for linking_rules in schema_rules.linking_rules:
        for rule in linking_rules.rules:
            complete_potential_vertex = generate_potential_vertex(rule.target_type)
            complete_edge = generate_potential_edge(source_vertex, complete_potential_vertex, rule.edge_type)
            stubbed_potential_vertex = generate_potential_vertex(rule.target_type, True)
            stubbed_edge = generate_potential_edge(source_vertex, stubbed_potential_vertex, rule.edge_type)
            results_sets.append(
                (source_vertex, rule.edge_type, [
                    {'edge': complete_edge, 'vertex': complete_potential_vertex},
                    {'edge': stubbed_edge, 'vertex': stubbed_potential_vertex}
                ])
            )
    return results_sets
