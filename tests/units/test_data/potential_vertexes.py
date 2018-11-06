from tests.units.test_data.actor_data import source_vertex, first_potential_vertex, second_potential_vertex
from tests.units.test_data.dynamo_data import edge_label, edge_internal_id, edge_properties, from_object_id, \
    to_object_id
from toll_booth.alg_obj.graph.ogm.regulators import PotentialEdge

change_potential_vertex = source_vertex
external_id_potential_vertex = first_potential_vertex
changelog_potential_vertex = second_potential_vertex
changed_edge = PotentialEdge(edge_label, edge_internal_id, edge_properties, from_object_id, to_object_id)
change_edge = PotentialEdge('_change_', 'sdgr234235', {}, to_object_id, 'c5bf540a3fb43491ad13e446cb9c3757')
