from toll_booth.alg_obj.graph.ogm.regulators import PotentialEdge


class TestPotentialEdge:
    def test_potential_edge_construction(self, test_edge):
        potential_edge = PotentialEdge(**test_edge[0])
        assert isinstance(potential_edge, PotentialEdge)

    def test_potential_edge_construction_from_json(self, test_edge):
        potential_edge = PotentialEdge.from_json(test_edge[0])
        assert isinstance(potential_edge, PotentialEdge)
