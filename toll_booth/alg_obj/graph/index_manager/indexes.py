import os

from boto3.dynamodb.conditions import Attr

from toll_booth.alg_obj.graph.ogm.regulators import MissingObjectProperty


class EmptyIndexException(Exception):
    def __init__(self, index_name):
        self._index_name = index_name

    @property
    def index_name(self):
        return self._index_name


class UniqueIndexViolationException(Exception):
    def __init__(self, index_name, indexed_object):
        self._index_name = index_name
        self._indexed_object = indexed_object

    @property
    def index_name(self):
        return self._index_name

    @property
    def indexed_object(self):
        return self._indexed_object


class MissingIndexException(Exception):
    def __init__(self, index_name):
        self._index_name = index_name


class MissingIndexedPropertyException(Exception):
    def __init__(self, index_name, indexed_fields, missing_fields):
        self._index_name = index_name
        self._indexed_fields = indexed_fields
        self._missing_fields = missing_fields


class AttemptedStubIndexException(Exception):
    def __init__(self, index_name, stub_object):
        self._index_name = index_name
        self._stub_object = stub_object


class Index:
    def __init__(self, index_name, indexed_fields, index_type, indexed_object_types):
        self._index_name = index_name
        self._indexed_fields = indexed_fields
        self._index_type = index_type
        self._indexed_object_types = indexed_object_types

    @property
    def index_name(self):
        return self._index_name

    @property
    def indexed_fields(self):
        return self._indexed_fields

    @property
    def index_type(self):
        return self._index_type

    @property
    def indexed_object_types(self):
        return self._indexed_object_types

    @property
    def conditional_statement(self):
        return [f'attribute_not_exists({x})' for x in self._indexed_fields]

    @property
    def is_unique(self):
        return self._index_type == 'unique'

    def check_for_missing_object_properties(self, graph_object):
        properties_dict = graph_object.for_index
        properties_dict.update(graph_object.object_properties)
        missing_properties = [x for x in self._indexed_fields if x not in properties_dict]
        if missing_properties:
            return missing_properties
        missing_values = [x for x, y in properties_dict.items() if isinstance(y, MissingObjectProperty)]
        if missing_values:
            return missing_values
        return False

    def check_object_type(self, graph_object):
        if '*' in self._indexed_object_types:
            return True
        if graph_object.object_type in self._indexed_object_types:
            return True
        return False


class UniqueIndex(Index):
    def __init__(self, index_name, indexed_fields, indexed_object_types):
        super().__init__(index_name, indexed_fields, 'unique', indexed_object_types)

    @classmethod
    def for_object_index(cls, **kwargs):
        index_name = kwargs.get('index_name', None)
        partition_key = kwargs.get('partition_key_name', None)
        hash_key = kwargs.get('hash_key_name', None)
        if not index_name:
            index_name = os.getenv('OBJECT_INDEX_NAME', 'leech_index')
        if not partition_key:
            partition_key = os.getenv('OBJECT_INDEX_PARTITION_KEY_NAME', 'sid_value')
        if not hash_key:
            hash_key = os.getenv('OBJECT_INDEX_HASH_KEY_NAME', 'identifier_stem')
        return cls(index_name, [partition_key, hash_key], ['*'])

    @classmethod
    def for_internal_id_index(cls, **kwargs):
        index_name = kwargs.get('index_name', None)
        if not index_name:
            index_name = os.getenv('INTERNAL_ID_INDEX_NAME', 'internal_id_index')
        internal_id_field_name = kwargs.get('internal_id_field_name', None)
        if not internal_id_field_name:
            internal_id_field_name = os.getenv('OBJECT_INTERNAL_ID_KEY_NAME', 'internal_id')
        return cls(index_name, [internal_id_field_name], ['*'])

    @classmethod
    def for_identifier_stem_index(cls, **kwargs):
        index_name = kwargs.get('index_name', None)
        if not index_name:
            index_name = os.getenv('IDENTIFIER_STEM_INDEX_NAME', 'identifier_stem_index')
        return cls(index_name, ['identifier_stem', 'id_value'], ['*'])

    @classmethod
    def for_link_index(cls, **kwargs):
        index_name = kwargs.get('index_name', None)
        index_fields = ['id_source', 'object_type', 'utc_link_time']
        if not index_name:
            index_name = os.getenv('LINK_INDEX_NAME', 'link_index')
        return cls(index_name, index_fields, [])

    @classmethod
    def for_fungal_index(cls, **kwargs):
        index_name = kwargs.get('index_name', None)
        if not index_name:
            index_name = os.getenv('FUNGAL_INDEX_NAME', 'fungal_index')
        fungal_stem_name = kwargs.get('fungal_stem_name')
        if not fungal_stem_name:
            fungal_stem_name = os.getenv('FUNGAL_STEM_NAME', 'fungal_stem')
        return cls(index_name, [fungal_stem_name, 'numeric_id_value'], ['ChangeLog'])


class NonUniqueIndex(Index):
    def __init__(self, index_name, indexed_fields, indexed_object_types):
        super().__init__(index_name, indexed_fields, 'non_unique', indexed_object_types)
