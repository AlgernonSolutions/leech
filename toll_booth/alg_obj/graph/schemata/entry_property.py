from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.graph.troubles import InvalidSchemaPropertyType


class SchemaPropertyEntry(AlgObject):
    _accepted_types = [
        'String', 'Integer', 'Float', 'DateTime'
    ]

    def __init__(self, property_name, property_data_type, sensitive=False):
        if property_data_type not in self._accepted_types:
            raise InvalidSchemaPropertyType(property_name, property_data_type, self._accepted_types)
        self._property_name = property_name
        self._property_data_type = property_data_type
        self._sensitive = sensitive

    @classmethod
    def parse(cls, property_dict):
        return cls(
            property_dict['property_name'],
            property_dict['property_data_type'],
            property_dict.get('sensitive', False)
        )

    @classmethod
    def parse_json(cls, json_dict):
        return cls.parse(json_dict)

    @property
    def sensitive(self):
        return self._sensitive

    @property
    def property_name(self):
        return self._property_name

    @property
    def property_data_type(self):
        return self._property_data_type


class EdgePropertyEntry(SchemaPropertyEntry):
    def __init__(self, property_name, property_data_type, property_source):
        super().__init__(property_name, property_data_type)
        self._property_source = property_source

    @classmethod
    def parse(cls, property_dict):
        return cls(
            property_dict['property_name'], property_dict['property_data_type'], property_dict['property_source'])

    @property
    def property_source(self):
        return self._property_source
