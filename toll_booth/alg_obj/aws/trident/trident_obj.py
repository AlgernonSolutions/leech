from datetime import datetime

from toll_booth.alg_obj import AlgObject


class TridentProperty(AlgObject):
    def __init__(self, property_label, property_value):
        self._property_label = property_label
        self._property_value = property_value

    def __len__(self):
        return 1

    def __str__(self):
        return str(self._property_value)

    def __int__(self):
        return int(self._property_value)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['property_label'],
            json_dict['property_value']
        )

    @property
    def to_gql(self):
        property_value = {
            '__typename': 'ObjectPropertyValue',
            'data_type': 'S',
            'property_value': self._property_value
        }
        if isinstance(self._property_value, int):
            property_value['data_type'] = 'I'
        if isinstance(self._property_value, datetime):
            property_value['data_type'] = 'DT'

        return {
            '__typename': 'ObjectProperty',
            'property_name': self._property_label,
            'property_value': property_value
        }

    @property
    def label(self):
        return self._property_label

    @property
    def value(self):
        return self._property_value


class TridentVertex(AlgObject):
    def __init__(self, vertex_id, vertex_label, vertex_properties=None):
        if not vertex_properties:
            vertex_properties = {}
        self._vertex_id = vertex_id
        self._vertex_label = vertex_label
        self._vertex_properties = vertex_properties

    def __len__(self):
        return 1

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['vertex_id'], json_dict['vertex_label'], json_dict['vertex_properties']
        )

    @property
    def to_gql(self):
        gql = {
            'internal_id': self.vertex_id,
            'vertex_type': self.vertex_label,
            '__typename': 'Vertex'
        }
        vertex_properties = []
        for property_name, vertex_property in self._vertex_properties.items():
            vertex_properties.append(vertex_property[0])
        if vertex_properties:
            gql['vertex_properties'] = vertex_properties
        return gql

    @property
    def vertex_id(self):
        return self._vertex_id

    @property
    def vertex_label(self):
        return self._vertex_label

    @property
    def vertex_properties(self):
        return self._vertex_properties

    @property
    def object_properties(self):
        vertex_properties = self.vertex_properties
        vertex_properties.update({
            'internal_id': self.vertex_id,
            'object_type': self.vertex_label
        })
        return vertex_properties

    def __iter__(self):
        return iter(self._vertex_properties)

    def __getitem__(self, item):
        return self._vertex_properties[item]

    def keys(self):
        return self._vertex_properties.keys()

    def values(self):
        return self._vertex_properties.values()

    def items(self):
        return self._vertex_properties.items()


class TridentEdge(AlgObject):
    def __init__(self, internal_id, label, from_vertex, to_vertex):
        self._internal_id = internal_id
        self._label = label
        self._from_vertex = from_vertex
        self._to_vertex = to_vertex

    def __len__(self):
        return 1

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['internal_id'], json_dict['label'],
            json_dict['from_vertex'], json_dict['to_vertex'],
        )

    @property
    def to_gql(self):
        return {
            'internal_id': self.internal_id,
            'edge_label': self.label,
            'from_vertex': self._from_vertex,
            'to_vertex': self._to_vertex,
            '__typename': 'Edge'
        }

    @property
    def internal_id(self):
        return self._internal_id

    @property
    def label(self):
        return self._label

    @property
    def in_id(self):
        return self._from_vertex.vertex_id

    @property
    def out_id(self):
        return self._to_vertex.vertex_id


class TridentPath(AlgObject):
    def __init__(self, labels, path_objects):
        self._labels = labels
        self._path_objects = path_objects

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['labels'],
            json_dict['path_objects']
        )

    @property
    def labels(self):
        return self._labels

    @property
    def path_objects(self):
        return self._path_objects


class TridentEdgeConnection(AlgObject):
    def __init__(self, edges, token, more, page_info=None):
        if not page_info:
            page_info = TridentPageInfo(token, more)
        self._edges = edges
        self._token = token
        self._more = more
        self._page_info = page_info

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['edges'], json_dict['token'], json_dict['more'], json_dict.get('page_info'))

    @property
    def page_info(self):
        return self._page_info

    @property
    def in_count(self):
        return len(self._edges['inbound'])

    @property
    def out_count(self):
        return len(self._edges['outbound'])

    @property
    def total_count(self):
        return len(self._edges['all'])

    @property
    def to_gql(self):
        return {
            '__typename': 'EdgeConnection',
            'edges': self._edges,
            'page_info': self._page_info,
            'total_count': self.total_count,
            'in_count': self.in_count,
            'out_count': self.out_count
        }


class TridentPageInfo(AlgObject):
    def __init__(self, token, more):
        self._token = token
        self._more = more

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['token'], json_dict['more'])

    @property
    def to_gql(self):
        return {
            'token': self._token,
            'more': self._more,
            '__typename': 'PageInfo'
        }
