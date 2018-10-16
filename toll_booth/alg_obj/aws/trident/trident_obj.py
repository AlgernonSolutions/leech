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
        return {
            '__typename': 'ObjectProperty',
            'property_name': self._property_label,
            'property_value': self._property_value
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
            '__typename': 'Vertex',
            'vertex_properties': []
        }
        for vertex_property in self._vertex_properties.values():
            first_property = vertex_property[0]
            gql['vertex_properties'].append(first_property.to_gql)
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
    def __init__(self, internal_id, label, in_v_internal_id, in_v_label, out_v_internal_id, out_v_label):
        self._internal_id = internal_id
        self._label = label
        self._in_v_internal_id = in_v_internal_id
        self._in_v_label = in_v_label
        self._out_v_internal_id = out_v_internal_id
        self._out_v_label = out_v_label

    def __len__(self):
        return 1

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['internal_id'], json_dict['label'], json_dict['in_v_internal_id'],
            json_dict['in_v_label'], json_dict['out_v_internal_id'], json_dict['out_v_label']
        )

    @property
    def to_gql(self):
        return {
            'internal_id': self.internal_id,
            'label': self.label,
            'in_id': self.in_id,
            'in_label': self.in_label,
            'out_id': self.out_id,
            'out_label': self.out_label,
            '__typename': self.label
        }

    @property
    def internal_id(self):
        return self._internal_id

    @property
    def label(self):
        return self._label

    @property
    def in_id(self):
        return self._in_v_internal_id

    @property
    def out_id(self):
        return self._out_v_internal_id

    @property
    def in_label(self):
        return self._in_v_label

    @property
    def out_label(self):
        return self._out_v_label

    @property
    def in_vertex_data(self):
        return {'internal_id': self._in_v_internal_id, 'label': self._in_v_label}

    @property
    def out_vertex_data(self):
        return {'internal_id': self._out_v_internal_id, 'label': self._out_v_label}


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
