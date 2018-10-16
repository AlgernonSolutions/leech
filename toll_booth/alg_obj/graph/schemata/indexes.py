from abc import ABC

from toll_booth.alg_obj import AlgObject


class SchemaIndexEntry(AlgObject, ABC):
    def __init__(self, index_name, index_type, index_properties):
        self._index_name = index_name
        self._index_type = index_type
        self._index_properties = index_properties

    @classmethod
    def parse(cls, index_dict):
        return cls(index_dict['index_name'], index_dict['index_type'], index_dict['index_properties'])

    @classmethod
    def parse_json(cls, json_dict):
        return cls.parse(json_dict)

    @property
    def index_name(self):
        return self._index_name

    @property
    def index_type(self):
        return self._index_type

    @property
    def index_properties(self):
        return self._index_properties


class SortedSetIndexEntry(SchemaIndexEntry):
    def __init__(self, index_name, score_field, key_fields):
        super().__init__(index_name, 'sorted_set', {'score': score_field, 'key': key_fields})
        self._score = score_field
        self._key = key_fields

    @classmethod
    def parse(cls, index_dict):
        try:
            return cls(index_dict['index_name'], index_dict['score'], index_dict['key'])
        except KeyError:
            index_properties = index_dict['index_properties']
            return cls(index_dict['index_name'], index_properties['score'], index_properties['key'])

    @property
    def score_field(self):
        return self._score

    @property
    def key_fields(self):
        return self._key


class UniqueIndexEntry(SchemaIndexEntry):
    def __init__(self, index_name, key_fields):
        super().__init__(index_name, 'unique', {'key': key_fields})
        self._key = key_fields

    @classmethod
    def parse(cls, index_dict):
        try:
            return cls(index_dict['index_name'], index_dict['key'])
        except KeyError:
            index_properties = index_dict['index_properties']
            return cls(index_dict['index_name'], index_properties['key'])

    @property
    def key_fields(self):
        return self._key
