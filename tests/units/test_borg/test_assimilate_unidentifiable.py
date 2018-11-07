import pytest

from toll_booth.alg_obj.forge.borgs import SevenOfNine
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex


@pytest.mark.borg
class TestAssimilateUnidentifiableOrder:
    def test_assimilate_unidentifiable_order_with_existing_vertexes(self, unidentifiable_assimilate_order, borg_test_environment):
        potential_object_type = unidentifiable_assimilate_order.potential_vertex.object_type
        borg_mocks = borg_test_environment(potential_object_type)
        borg = SevenOfNine(unidentifiable_assimilate_order)
        borg.assimilate()
        find_vertexes_mock = borg_mocks['find_potential_vertexes']
        set_results = borg_mocks['set_assimilation_results']
        set_vertex = borg_mocks['set_assimilated_vertex']
        self._assert_find_existing_vertexes_called_correctly(
            find_vertexes_mock, unidentifiable_assimilate_order.potential_vertex)
        self._assert_set_found_vertexes_assimilation_results_correctly(
            set_results, unidentifiable_assimilate_order.source_vertex, unidentifiable_assimilate_order.rule_entry, True
        )
        self._assert_no_new_vertexes_set(
            set_vertex
        )

    def test_assimilate_stubbed_unidentifiable_order_without_existing_vertexes(self, unidentifiable_assimilate_order, borg_test_environment):
        borg_mocks = borg_test_environment()
        borg = SevenOfNine(unidentifiable_assimilate_order)
        borg.assimilate()
        find_vertexes_mock = borg_mocks['find_potential_vertexes']
        set_results = borg_mocks['set_assimilation_results']
        set_vertex = borg_mocks['set_assimilated_vertex']
        self._assert_find_existing_vertexes_called_correctly(
            find_vertexes_mock, unidentifiable_assimilate_order.potential_vertex)
        self._assert_set_found_vertexes_assimilation_results_correctly(
            set_results, unidentifiable_assimilate_order.source_vertex, unidentifiable_assimilate_order.rule_entry, True
        )
        self._assert_new_stub_vertexes_set(
            set_vertex, unidentifiable_assimilate_order.potential_vertex
        )

    def test_assimilate_unidentifiable_order_without_existing_vertexes(self):
        pass

    @classmethod
    def _assert_find_existing_vertexes_called_correctly(cls, find_vertexes_mock, potential_vertex):
        find_vertexes_mock.assert_called_once()
        source_vertex_properties = potential_vertex.object_properties
        searched_vertex_properties = find_vertexes_mock.call_args[0][1]
        searched_vertex_type = find_vertexes_mock.call_args[0][0]
        source_vertex_type = potential_vertex.object_type
        assert source_vertex_properties == searched_vertex_properties
        assert searched_vertex_type == source_vertex_type

    @classmethod
    def _assert_set_found_vertexes_assimilation_results_correctly(cls, set_results_mock, source_vertex, rule_entry, found_any=False):
        set_results_mock.assert_called_once()
        set_edge_type = set_results_mock.call_args[0][0]
        identified_vertexes = set_results_mock.call_args[0][1]
        set_identifier_stem = set_results_mock.call_args[1]['identifier_stem']
        set_id_value = set_results_mock.call_args[1]['id_value']
        if found_any:
            for vertex in identified_vertexes:
                assert 'edge' in vertex
                assert 'vertex' in vertex
        if not found_any:
            assert identified_vertexes == []
        assert rule_entry.edge_type == set_edge_type
        assert source_vertex.identifier_stem == set_identifier_stem
        assert source_vertex.id_value == set_id_value

    @classmethod
    def _assert_no_new_vertexes_set(cls, set_vertex_mock):
        set_vertex_mock.assert_not_called()

    @classmethod
    def _assert_new_stub_vertexes_set(cls, set_vertex_mock, potential_vertex):
        set_vertex_mock.assert_called_once()
        set_vertex_args = set_vertex_mock.call_args[0]
        set_vertex_kwargs = set_vertex_mock.call_args[1]
        assert set_vertex_args[1] is True
        assert set_vertex_kwargs == {
            'identifier_stem': None,
            'id_value': None
        }
        new_stub_vertex = set_vertex_args[0]
        assert isinstance(new_stub_vertex, PotentialVertex)
        assert new_stub_vertex is potential_vertex
