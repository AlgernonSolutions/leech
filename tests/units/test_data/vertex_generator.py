import random
from datetime import datetime

from toll_booth.alg_obj.graph.ogm.regulators import VertexRegulator, EdgeRegulator
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry, SchemaEdgeEntry


def generate_potential_vertex(object_type, missing_properties=False):
    schema_entry = SchemaVertexEntry.retrieve(object_type)
    vertex_regulator = VertexRegulator(schema_entry)
    vertex_properties = {}
    object_properties = schema_entry.vertex_properties
    for property_name, object_property in object_properties.items():
        vertex_properties[property_name] = _generate_object_property(object_property)
    if missing_properties:
        num_missing = random.choice(list(range(1, len(vertex_properties)-1)))
        for _ in range(num_missing):
            blank_key = random.choice(list(vertex_properties.keys()))
            del vertex_properties[blank_key]
    return vertex_regulator.create_potential_vertex(vertex_properties)


def generate_potential_edge(source_vertex, other_vertex, edge_type):
    schema_entry = SchemaEdgeEntry.retrieve(edge_type)
    edge_regulator = EdgeRegulator(schema_entry)
    inbound = source_vertex.object_type in schema_entry.to_types
    extracted_data = _generate_extracted_data(schema_entry.edge_properties)
    potential_edge = edge_regulator.generate_potential_edge(source_vertex, other_vertex, extracted_data, inbound)
    return potential_edge


def _generate_extracted_data(edge_properties):
    extracted_data = {}
    for property_name, edge_property in edge_properties.items():
        property_source = edge_property.property_source
        if property_source['source_type'] == 'extraction':
            extraction_name = property_source['extraction_name']
            if extraction_name not in extracted_data:
                extracted_data[extraction_name] = []
            property_value = _generate_object_property(edge_property)
            extracted_data[extraction_name].append({property_name: property_value})
    return extracted_data


def _generate_object_property(object_property):
    property_data_type = object_property.property_data_type
    if property_data_type == 'Number':
        return random.randint(1, 999999)
    if property_data_type == 'DateTime':
        return datetime.now()
    if property_data_type == 'String':
        strings = [
            'this is a random string',
            'now, now, this is a proper random string',
            'will you two shut it!',
            'no, you shut it!',
            'yeah! you are not the boss of us!',
            '',
            '#$@1',
            '##########',
            '#@%@#$%*@$@',
            'that is what i thought'
        ]
        return random.choice(strings)
    raise NotImplementedError(f'do not know how to generate test properties for data_type: {property_data_type}')
