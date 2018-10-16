class ObjectIdentifier:
    def __init__(self, object_type, id_source, id_name, id_type=None):
        if object_type == 'ExternalId' and id_type is None:
            raise NotImplementedError('can not monitor with current configurations, object type ExternalId must have '
                                      'a specified id_type')
        self._object_type = object_type
        self._id_source = id_source
        self._id_name = id_name
        self._id_type = id_type

    @property
    def object_type(self):
        return self._object_type

    @property
    def to_json(self):
        return {
            'object_type': self._object_type,
            'id_source': self._id_source,
            'id_name': self._id_name,
            'id_type': self._id_type
        }
