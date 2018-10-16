from toll_booth.alg_obj import AlgObject
from abc import ABC, abstractmethod

from toll_booth.alg_obj.utils import get_subclasses


class VertexRules(AlgObject):
    def __init__(self, linking_rules):
        self._linking_rules = linking_rules

    @classmethod
    def get(cls, vertex_type):
        from toll_booth.alg_obj.aws.aws_obj.sapper import SchemaWhisperer
        whisperer = SchemaWhisperer()
        rules_query = whisperer.get_rules(vertex_type)
        try:
            rules = rules_query['Items'][0]['rules']
        except IndexError:
            raise Exception('no rules returned for object type: %s' % vertex_type)
        return cls.parse(rules)

    @classmethod
    def parse(cls, rules):
        linking_rules_list = rules['linking_rules']
        for entry in linking_rules_list:
            if isinstance(entry, VertexLinkRuleSet):
                return cls(linking_rules_list)
        return cls([VertexLinkRuleSet.parse(
            x['vertex_specifiers'], x['outbound'], x['inbound']) for x in rules['linking_rules']])

    @classmethod
    def parse_json(cls, json_dict):
        return cls.parse(json_dict)

    @property
    def linking_rules(self):
        return self._linking_rules


class VertexLinkRuleSet(AlgObject):
    def __init__(self, vertex_specifiers, outbound_rules, inbound_rules):
        self._vertex_specifiers = vertex_specifiers
        self._outbound_rules = outbound_rules
        self._inbound_rules = inbound_rules

    @classmethod
    def parse(cls, vertex_specifiers, outbound_rules, inbound_rules):
        return cls(
            vertex_specifiers,
            [VertexLinkRuleEntry.parse(x, inbound=False) for x in outbound_rules],
            [VertexLinkRuleEntry.parse(x, inbound=True) for x in inbound_rules]
        )

    @classmethod
    def parse_json(cls, json_dict):
        return cls.parse(
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
    def parse(cls, rule_dict, inbound):
        try:
            target_specifiers = _parse_link_specifiers(rule_dict['target_specifiers'])
        except TypeError:
            try:
                target_specifiers = rule_dict['target_specifiers']
            except TypeError:
                return rule_dict
        return cls(
            rule_dict['target_type'],
            rule_dict['edge_type'],
            rule_dict['target_constants'],
            target_specifiers,
            rule_dict['if_absent'],
            inbound
        )

    @classmethod
    def parse_json(cls, json_dict):
        return cls.parse(json_dict, json_dict['inbound'])

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


class TargetSpecifier(AlgObject, ABC):
    def __init__(self, specifier_name, specifier_type):
        self._specifier_name = specifier_name
        self._specifier_type = specifier_type

    @property
    def specifier_name(self):
        return self._specifier_name

    @property
    def specifier_type(self):
        return self._specifier_type

    @abstractmethod
    def generate_specifiers(self, **kwargs):
        raise NotImplementedError()


class SharedPropertySpecifier(TargetSpecifier):
    def __init__(self, specifier_name, shared_properties):
        super().__init__(specifier_name, 'shared_property')
        self._shared_properties = shared_properties

    @classmethod
    def parse_json(cls, json_dict):
        if json_dict['specifier_type'] != 'shared_property':
            raise RuntimeError()
        return cls(
            json_dict['specifier_name'], json_dict['shared_properties']
        )

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
        if json_dict['specifier_type'] != 'extraction':
            raise RuntimeError()
        return cls(
            json_dict['specifier_name'], json_dict['extracted_properties']
        )

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
        if json_dict['specifier_type'] != 'function':
            raise RuntimeError()
        return cls(
            json_dict['specifier_name'], json_dict['function_name'], json_dict['extracted_properties']
        )

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


def _parse_link_specifiers(specifiers):
    specifier_types = get_subclasses(TargetSpecifier)
    selected_specifiers = []
    for specifier in specifiers:
        for possible_specifier in specifier_types.values():
            try:
                selected_specifiers.append(possible_specifier.parse_json(specifier))
                break
            except RuntimeError:
                continue
        else:
            raise NotImplementedError('could not generate a target specifier from the provided schema')
    return selected_specifiers
