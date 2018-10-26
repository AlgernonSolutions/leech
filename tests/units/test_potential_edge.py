from datetime import datetime

from toll_booth.alg_obj.graph.ogm.regulators import PotentialEdge, IdentifierStem

from_object_id = '1233'
to_object_id = '3321'
edge_internal_id = '5678'
edge_properties = {'changed_target': {'change_date_utc': datetime.now().isoformat()}}
edge_label = '_changed_'
edge = {
    'object_type': edge_label,
    'object_properties': edge_properties,
    'internal_id': edge_internal_id,
    'from_object': from_object_id,
    'to_object': to_object_id
}


class TestPotentialEdge:
    def test_potential_edge_construction(self):
        potential_edge = PotentialEdge(edge_label, edge_internal_id, edge_properties, from_object_id, to_object_id)
        assert isinstance(potential_edge, PotentialEdge)

    def test_potential_edge_construction_from_json(self):
        potential_edge = PotentialEdge.from_json(edge)
        assert isinstance(potential_edge, PotentialEdge)

    def test_edge_identifier_stem_creation(self):
        potential_edge = PotentialEdge.from_json(edge)
        identifier_stem = potential_edge.identifier_stem
        assert isinstance(identifier_stem, IdentifierStem)
