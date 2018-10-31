import os
from datetime import datetime

from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

partition_key = os.getenv('PARTITION_KEY', 'identifier_stem')
sort_key = os.getenv('SORT_KEY', 'sid_value')

id_range = range(10, 15)

vertex_type = 'ExternalId'
vertex_id_value = 1001
vertex_id_value_field = 'id_value'
vertex_internal_id = '11112222'
vertex_properties = {'property_1': 'red', 'property_2': 3}
vertex_identifier_stem = IdentifierStem('vertex', vertex_type, vertex_properties)
seed_vertex = {
    'identifier_stem': str(vertex_identifier_stem),
    'sid_value': str(vertex_id_value),
    'id_value': vertex_id_value
}
vertex = {
    'identifier_stem': str(vertex_identifier_stem),
    'sid_value': str(vertex_id_value),
    'id_value': vertex_id_value,
    'object_type': vertex_type,
    'if_missing': 'pass',
    'id_value_field': vertex_id_value_field,
    'is_edge': False,
    'completed': False,
    'internal_id': vertex_internal_id,
    'object_properties': vertex_properties
}
vertex_key = {partition_key: str(vertex_identifier_stem), sort_key: str(vertex_id_value)}

other_vertex_type = 'ExternalId'
other_id_value = 2002
other_internal_id = '222111111'
other_vertex_properties = {'property_1': 'blue', 'property_2': 6}
other_vertex = {
    'identifier_stem': str(vertex_identifier_stem),
    'sid_value': str(other_id_value),
    'id_value': other_id_value,
    'object_type': vertex_type,
    'if_missing': 'pass',
    'id_value_field': vertex_id_value_field,
    'is_edge': False,
    'completed': False,
    'internal_id': other_internal_id,
    'object_properties': other_vertex_properties
}

from_object_id = vertex_internal_id
to_object_id = other_internal_id
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
edge_extracted_data = {
    'changed_target': [{
        'change_date_utc': datetime.now()
    }]
}
edge_key = {partition_key: str(IdentifierStem('edge', edge_label)), sort_key: edge_internal_id}
index_name = 'id_values'

os.environ['partition_key'] = partition_key
os.environ['sort_key'] = sort_key
os.environ['index_name'] = index_name
