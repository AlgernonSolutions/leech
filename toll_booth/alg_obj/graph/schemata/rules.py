from toll_booth.alg_obj import AlgObject


class VertexRules(AlgObject):
    def __init__(self, linking_rules=None):
        if not linking_rules:
            linking_rules = []
        self._linking_rules = linking_rules

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['linking_rules'])

    @property
    def linking_rules(self):
        return self._linking_rules

    def add_rule_set(self, rule_set):
        self._linking_rules.append(rule_set)


class VertexLinkRuleSet(AlgObject):
    def __init__(self, vertex_specifiers, outbound_rules, inbound_rules):
        self._vertex_specifiers = vertex_specifiers
        self._outbound_rules = outbound_rules
        self._inbound_rules = inbound_rules

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['vertex_specifiers'],
            json_dict['outbound_rules'],
            json_dict['inbound_rules']
        )

    @property
    def rules(self):
        rules = self._outbound_rules + self._inbound_rules
        return rules

    @property
    def vertex_specifiers(self):
        return self._vertex_specifiers


class VertexLinkRuleEntry(AlgObject):
    def __init__(self, target_type, edge_type, target_constants, target_specifiers, if_absent, inbound=False, **kwargs):
        self._target_type = target_type
        self._edge_type = edge_type
        self._target_constants = target_constants
        self._target_specifiers = target_specifiers
        self._if_absent = if_absent
        self._inbound = inbound
        try:
            function_name = kwargs['function_name']
        except KeyError:
            function_name = None
        self._function_name = function_name

    @classmethod
    def parse_json(cls, json_dict):
        return cls(
            json_dict['target_type'],
            json_dict['edge_type'],
            json_dict['target_constants'],
            json_dict['target_specifiers'],
            json_dict['if_absent'],
            json_dict.get('inbound', False)
        )

    @property
    def target_specifiers(self):
        return self._target_specifiers

    @property
    def target_constants(self):
        return self._target_constants

    @property
    def target_type(self):
        return self._target_type

    @property
    def edge_type(self):
        return self._edge_type

    @property
    def if_absent(self):
        return self._if_absent

    @property
    def inbound(self):
        return self._inbound

    @property
    def function_name(self):
        return self._function_name

    @property
    def is_stub(self):
        return self._if_absent == 'stub'

    @property
    def is_create(self):
        return self._if_absent == 'create'

    @property
    def is_pass(self):
        return self._if_absent == 'pass'

    def __str__(self):
        return self._edge_type


class TargetSpecifier(AlgObject):
    def __init__(self, specifier_name, specifier_type):
        self._specifier_name = specifier_name
        self._specifier_type = specifier_type

    @property
    def specifier_name(self):
        return self._specifier_name

    @property
    def specifier_type(self):
        return self._specifier_type

    @classmethod
    def parse_json(cls, json_dict):
        raise NotImplementedError()

    def generate_specifiers(self, **kwargs):
        raise NotImplementedError()


class SharedPropertySpecifier(TargetSpecifier):
    def __init__(self, specifier_name, shared_properties):
        super().__init__(specifier_name, 'shared_property')
        self._shared_properties = shared_properties

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['specifier_name'], json_dict['shared_properties'])

    @property
    def extracted_properties(self):
        return self._shared_properties

    def generate_specifiers(self, **kwargs):
        source_vertex = kwargs['source_vertex']
        target_constants = kwargs['target_constants']
        shared_properties = target_constants.copy()
        for field_name in self._shared_properties:
            if field_name not in shared_properties:
                shared_properties[field_name] = source_vertex[field_name]
        return [shared_properties]


class ExtractionSpecifier(TargetSpecifier):
    def __init__(self, specifier_name, extracted_properties):
        super().__init__(specifier_name, 'extraction')
        self._extracted_properties = extracted_properties

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['specifier_name'], json_dict['extracted_properties'])

    def generate_specifiers(self, **kwargs):
        target_constants = kwargs['target_constants']
        specifiers = []
        extracted_data = kwargs['extracted_data'][self._specifier_name]
        for entry in extracted_data:
            specified = target_constants.copy()
            for field_name in self._extracted_properties:
                if field_name not in specified:
                    specified[field_name] = entry[field_name]
            specifiers.append(specified)
        return specifiers

    @property
    def extracted_properties(self):
        return self._extracted_properties


class FunctionSpecifier(TargetSpecifier):
    def __init__(self, specifier_name, function_name, extracted_properties):
        super().__init__(specifier_name, 'function')
        self._function_name = function_name
        self._extracted_properties = extracted_properties

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['specifier_name'], json_dict['function_name'], json_dict['extracted_properties'])

    @property
    def function_name(self):
        return self._function_name

    @property
    def extracted_properties(self):
        return self._extracted_properties

    def generate_specifiers(self, **kwargs):
        from toll_booth.alg_obj.forge import specifiers
        function_name = self._function_name
        try:
            specifier_function = getattr(specifiers, function_name)
        except AttributeError:
            raise NotImplementedError('specifier function named: %s is not registered with the system' % function_name)
        return specifier_function(**kwargs)
