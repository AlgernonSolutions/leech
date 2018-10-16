import json
from abc import ABC

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.serializers import AlgEncoder


class MetalOrder(AlgObject, ABC):
    def __init__(self, action_name):
        self._action_name = action_name

    @property
    def schema_entry(self):
        return getattr(self, '_schema_entry', None)

    @property
    def to_work(self):
        task_args = {'metal_order': self}
        return {
            'task_name': getattr(self, '_action_name'),
            'task_args': json.dumps(task_args, cls=AlgEncoder)
        }

    @property
    def action_name(self):
        return self._action_name


class ExtractObjectOrder(MetalOrder):
    def __init__(self, id_value, extraction_source, extraction_properties, schema_entry):
        super().__init__('extract')
        self._id_value = id_value
        self._schema_entry = schema_entry
        self._extraction_source = extraction_source
        extraction_properties['id_value'] = id_value
        self._extraction_properties = extraction_properties

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['id_value'], json_dict['extraction_source'],
            json_dict['extraction_properties'], json_dict['schema_entry']
        )

    @property
    def to_json(self):
        return {
            'id_value': self._id_value,
            'schema_entry': self._schema_entry,
            'extraction_source': self._extraction_source,
            'extraction_properties': self._extraction_properties,
            'action_name': self._action_name
        }

    @property
    def extraction_properties(self):
        return self._extraction_properties

    @property
    def extraction_source(self):
        return self._extraction_source

    @property
    def id_value(self):
        return self._id_value


class TransformObjectOrder(MetalOrder):
    def __init__(self, extracted_data, schema_entry):
        super().__init__('transform')
        self._extracted_data = extracted_data
        self._schema_entry = schema_entry

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['extracted_data'], json_dict['schema_entry'])

    @property
    def to_json(self):
        return {
            'extracted_data': self._extracted_data,
            'schema_entry': self._schema_entry,
            'action_name': self._action_name
        }

    @property
    def extracted_data(self):
        return self._extracted_data


class AssimilateObjectOrder(MetalOrder):
    def __init__(self, source_vertex, potential_vertex, rule_entry, extracted_data):
        super().__init__('assimilate')
        self._source_vertex = source_vertex
        self._potential_vertex = potential_vertex
        self._rule_entry = rule_entry
        self._extracted_data = extracted_data

    @classmethod
    def for_source_vertex(cls, source_vertex, extracted_data):
        return cls(source_vertex, source_vertex, None, extracted_data)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['source_vertex'],
            json_dict['potential_vertex'],
            json_dict['rule_entry'],
            json_dict['extracted_data']
        )

    @property
    def source_vertex(self):
        return self._source_vertex

    @property
    def potential_vertex(self):
        return self._potential_vertex

    @property
    def rule_entry(self):
        return self._rule_entry

    @property
    def extracted_data(self):
        return self._extracted_data


class LoadObjectOrder(MetalOrder):
    def __init__(self, vertex, edge):
        super().__init__('load')
        self._vertex = vertex
        self._edge = edge

    @classmethod
    def for_source_vertex(cls, source_vertex):
        return cls(source_vertex, None)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['vertex'],
            json_dict['edge']
        )

    @property
    def vertex(self):
        return self._vertex

    @property
    def edge(self):
        return self._edge

    @property
    def to_json(self):
        return {
            "vertex": self._vertex,
            "edge": self._edge,
            "action_name": self._action_name
        }


class ProcessObjectOrder(MetalOrder):
    def __init__(self, vertex):
        super().__init__('process')
        self._vertex = vertex

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['vertex'])

    @property
    def vertex(self):
        return self._vertex
