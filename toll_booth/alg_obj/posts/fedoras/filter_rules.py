from toll_booth.alg_obj import AlgObject


class FilteringRule(AlgObject):
    def __init__(self, filter_type):
        self._filter_type = filter_type

    @property
    def filter_type(self):
        return self._filter_type

    @classmethod
    def parse_json(cls, json_dict):
        if json_dict['filter_type'] == 'function':
            return FunctionFilterRule.parse_json(json_dict)
        return cls(json_dict['filter_type'])

    @classmethod
    def parse_from_schema_entry(cls, schema_entry):
        return cls.parse_json(schema_entry)


class FunctionFilterRule(FilteringRule):
    def __init__(self, fn_name):
        self._fn_name = fn_name
        super().__init__('function')

    @property
    def fn_name(self):
        return self._fn_name

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['fn_name'])

    @classmethod
    def parse_from_schema_entry(cls, schema_entry):
        return cls.parse_json(schema_entry)
