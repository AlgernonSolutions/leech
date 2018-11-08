import random
from datetime import datetime

from toll_booth.alg_obj.graph.ogm.regulators import VertexRegulator
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry


def generate_potential_vertex(object_type, missing_properties=False):
    schema_entry = SchemaVertexEntry.get(object_type)
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
