from toll_booth.alg_obj.graph.ogm.regulators import ObjectRegulator
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex


class RuleArbiter:
    def __init__(self, source_vertex, schema_entry):
        self._schema_entry = schema_entry
        self._rules = schema_entry.rules
        self._source_vertex = source_vertex

    @property
    def source_vertex(self):
        return self._source_vertex

    def process_rules(self, extracted_data):
        vertexes = []
        linking_rules = self._rules.linking_rules
        for rule_set in linking_rules:
            vertex_specifiers = rule_set.vertex_specifiers
            # TODO implement vertex testing
            if not self.test_vertex_specifiers(extracted_data, vertex_specifiers):
                continue
            for rule_entry in rule_set.rules:
                executor = ArbiterExecutor(self, rule_entry)
                vertexes.extend(executor.generate_potential_vertexes(extracted_data))
        return vertexes

    @staticmethod
    def test_vertex_specifiers(extracted_data, vertex_specifiers):
        return True


class ArbiterExecutor:
    def __init__(self, rule_arbiter, rule_entry):
        self._rule_arbiter = rule_arbiter
        self._source_vertex = rule_arbiter.source_vertex
        self._rule_entry = rule_entry
        self._target_type = rule_entry.target_type
        self._regulator = ObjectRegulator.get_for_object_type(rule_entry.target_type, rule_entry)

    @property
    def is_stub(self):
        return self._rule_entry.is_stub

    @property
    def if_missing(self):
        return self._rule_entry.if_absent

    def generate_potential_vertexes(self, extracted_data):
        target_specifiers = self._rule_entry.target_specifiers
        target_constants = self.derive_target_constants(self._rule_entry.target_constants)
        specifiers = []
        for specifier_executor in target_specifiers:
            specifier_kwargs = {
                'extracted_data': extracted_data,
                'rule_entry': self._rule_entry,
                'target_constants': target_constants,
                'source_vertex': self._source_vertex,
                'specifier': specifier_executor
            }
            generated_specifiers = specifier_executor.generate_specifiers(**specifier_kwargs)
            for generated_specifier in generated_specifiers:
                internal_id = self._regulator.create_internal_id(generated_specifier)
                object_properties = self._regulator.standardize_object_properties(generated_specifier)
                specified_object = PotentialVertex(self._target_type, internal_id, object_properties, self.if_missing)
                specifiers.append((specified_object, self._rule_entry))
        return specifiers

    def derive_target_constants(self, target_constants):
        derived_constants = {}
        for target_constant in target_constants:
            constant_name, constant_value = self._derive_target_constant(target_constant)
            derived_constants[constant_name] = constant_value
        return derived_constants

    def _derive_target_constant(self, target_constant_entry):
        constant_name = target_constant_entry['constant_name']
        constant_value = target_constant_entry['constant_value']
        if "source." in constant_value:
            source_field_name = constant_value.split('.')[1]
            constant_value = self._source_vertex[source_field_name]
        return constant_name, constant_value
