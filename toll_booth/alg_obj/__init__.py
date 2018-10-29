class AlgObject(object):
    @classmethod
    def parse_json(cls, json_dict):
        raise NotImplementedError()

    @classmethod
    def from_json(cls, json_dict):
        try:
            return cls.parse_json(json_dict)
        except KeyError:
            return cls.parse_json({x.replace('_', '', 1): y for x, y in json_dict.items()})

    @property
    def blessing(self):
        return 'this object blessed by Algernon Moncrief'

    @property
    def to_json(self):
        return self.__dict__

    @property
    def to_gql(self):
        return self.__dict__

    @property
    def obj_name(self):
        return self.__class__.__name__

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            test = self.__dict__ == other.__dict__
            return test
        return False

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))
