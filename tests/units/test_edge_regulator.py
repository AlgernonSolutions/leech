from toll_booth.alg_obj.graph.ogm.regulators import ObjectRegulator, PotentialVertex, PotentialEdge


class TestEdgeRegulator:
    def test_generate_potential_edge(self, test_edge):
        edge_data = test_edge[0]
        source_vertex = PotentialVertex.from_json(test_edge[1])
        other_vertex = PotentialVertex.from_json(test_edge[2])
        regulator = ObjectRegulator.get_for_object_type(edge_data['object_type'])
        potential_edge = regulator.generate_potential_edge(source_vertex, other_vertex, edge_data, False)
        assert isinstance(potential_edge, PotentialEdge)
        assert potential_edge.from_object == source_vertex.internal_id
        assert potential_edge.to_object == other_vertex.internal_id
