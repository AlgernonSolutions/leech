from toll_booth.alg_obj import AlgObject


class QueryArgument(AlgObject):
    def __init__(self, arg_name, arg_type):
        self._arg_name = arg_name
        self._arg_type = arg_type

    @property
    def arg_name(self):
        return self._arg_name

    @property
    def arg_type(self):
        return self._arg_type

    @classmethod
    def parse_from_schema_entry(cls, arg_name, schema_entry):
        arg_type = schema_entry['arg_type']
        if arg_type == 'static':
            return StaticQueryArgument(arg_name, schema_entry['arg_value'])
        if arg_type == 'provided':
            return ProvidedQueryArgument(arg_name, schema_entry['arg_name'])
        if arg_type == 'function':
            return FunctionQueryArgument(arg_name, schema_entry['fn_name'], schema_entry['fn_args'])
        return cls(arg_name, 'unknown')

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['arg_name'], json_dict['arg_type'])

    def __call__(self, *args, **kwargs):
        raise NotImplementedError()


class StaticQueryArgument(QueryArgument):
    def __init__(self, arg_name, arg_value):
        self._arg_value = arg_value
        super().__init__(arg_name, 'static')

    def __call__(self, *args, **kwargs):
        return self._arg_value

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['arg_name'], json_dict['arg_value'])


class ProvidedQueryArgument(QueryArgument):
    def __init__(self, arg_name, provided_name):
        self._provided_name = provided_name
        super().__init__(arg_name, 'provided')

    def __call__(self, *args, **kwargs):
        return kwargs[self._provided_name]

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['arg_name'], json_dict['provided_name'])


class FunctionQueryArgument(QueryArgument):
    def __init__(self, arg_name, fn_name, fn_args):
        self._fn_name = fn_name
        self._fn_args = fn_args
        super().__init__(arg_name, 'function')

    def __call__(self, *args, **kwargs):
        fn_value = kwargs[self._fn_name](**self._fn_args)
        return fn_value

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['arg_name'], json_dict['fn_name'], json_dict['fn_args'])
