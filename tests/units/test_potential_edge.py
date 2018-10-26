from toll_booth.alg_obj.graph.ogm.regulators import PotentialEdge


class TestPotentialEdge:
    def test_potential_edge_construction(self, test_edge):
        edge_data = test_edge[0]
        edge_properties = {'changed_target': edge_data['changed_target']}
        potential_edge = PotentialEdge(
            edge_data['object_type'], edge_data['internal_id'], edge_properties,
            edge_data['from_object'], edge_data['to_object'])
        assert isinstance(potential_edge, PotentialEdge)

    def test_potential_edge_construction_from_json(self, test_edge):
        potential_edge = PotentialEdge.from_json(test_edge[0])
        assert isinstance(potential_edge, PotentialEdge)
