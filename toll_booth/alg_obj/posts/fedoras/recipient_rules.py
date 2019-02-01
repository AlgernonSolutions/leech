from toll_booth.alg_obj import AlgObject


class RecipientRule(AlgObject):
    @classmethod
    def parse_json(cls, json_dict):
        if 'primary_only' and 'number_levels' in json_dict:
            return SupervisorAggregationRule.parse_json(json_dict)
        return RecipientAttributeRule.parse_json(json_dict)

    @classmethod
    def parse_from_schema_entry(cls, schema_entry):
        return cls.parse_json(schema_entry)


class RecipientAttributeRule(RecipientRule):
    def __init__(self, attribute_name, attribute_values):
        self._attribute_name = attribute_name
        self._attribute_values = attribute_values

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['attribute_name'], json_dict['attribute_values'])

    @classmethod
    def parse_from_schema_entry(cls, schema_entry):
        return cls.parse_json(schema_entry)


class SupervisorAggregationRule(RecipientRule):
    def __init__(self, primary_only, number_levels):
        self._primary_only = primary_only
        self._number_levels = number_levels

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['primary_only'], json_dict['number_levels'])

    @classmethod
    def parse_from_schema_entry(cls, schema_entry):
        return cls.parse_json(schema_entry)


class ReportRecipientRules(AlgObject):
    def __init__(self, direct_recipient_attributes, aggregation_chain=None):
        if not aggregation_chain:
            aggregation_chain = []
        self._direct_recipient_attributes = direct_recipient_attributes
        self._aggregation_chain = aggregation_chain

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['direct_recipient_attributes'], json_dict.get('aggregation_chain', None))

    @classmethod
    def parse_from_schema_entry(cls, schema_entry):
        if not schema_entry:
            return None
        direct_recipient_data = schema_entry['direct_recipient_attributes']
        aggregation_chain_data = schema_entry.get('aggregation_chain', [])
        direct_rules = [RecipientRule.parse_from_schema_entry(x) for x in direct_recipient_data]
        aggregation_rules = [RecipientRule.parse_from_schema_entry(x) for x in aggregation_chain_data]
        return cls(direct_rules, aggregation_rules)
