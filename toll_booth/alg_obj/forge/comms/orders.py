import json
from abc import ABC

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
from toll_booth.alg_obj.serializers import AlgEncoder


class MetalOrder(AlgObject, ABC):
    def __init__(self, action_name, identifier_stem):
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        self._action_name = action_name
        self._identifier_stem = str(identifier_stem)

    @property
    def schema_entry(self):
        return getattr(self, '_schema_entry', None)

    @property
    def identifier_stem(self):
        return self._identifier_stem

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
    def __init__(self, identifier_stem, id_value, extraction_source, extraction_properties, schema_entry):
        super().__init__('extract', identifier_stem)
        self._id_value = id_value
        self._schema_entry = schema_entry
        self._extraction_source = extraction_source
        extraction_properties['id_value'] = id_value
        self._extraction_properties = extraction_properties

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['identifier_stem'], json_dict['id_value'], json_dict['extraction_source'],
            json_dict['extraction_properties'], json_dict['schema_entry']
        )

    @property
    def to_json(self):
        return {
            'identifier_stem': self._identifier_stem,
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
    def __init__(self, identifier_stem, id_value, extracted_data, schema_entry):
        super().__init__('transform', identifier_stem)
        self._id_value = id_value
        self._extracted_data = extracted_data
        self._schema_entry = schema_entry

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['identifier_stem'], json_dict['id_value'],
            json_dict['extracted_data'], json_dict['schema_entry'])

    @property
    def to_json(self):
        return {
            'identifier_stem': self._identifier_stem,
            'id_value': self._id_value,
            'extracted_data': self._extracted_data,
            'schema_entry': self._schema_entry,
            'action_name': self._action_name
        }

    @property
    def extracted_data(self):
        return self._extracted_data

    @property
    def id_value(self):
        return self._id_value


class AssimilateObjectOrder(MetalOrder):
    def __init__(self, source_vertex, potential_vertex, rule_entry, extracted_data):
        super().__init__('assimilate', source_vertex.identifier_stem)
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
