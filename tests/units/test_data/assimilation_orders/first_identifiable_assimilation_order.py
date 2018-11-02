import datetime
from decimal import Decimal

from toll_booth.alg_obj.forge.comms.orders import AssimilateObjectOrder
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex, IdentifierStem
from toll_booth.alg_obj.graph.schemata.rules import FunctionSpecifier, VertexLinkRuleEntry

rule_constants = [{'constant_name': 'id_source', 'constant_value': 'source.id_source'}]
rule_specifiers = FunctionSpecifier(
    'changed_target', 'derive_change_targeted', ['id_source', 'id_type', 'id_name', 'id_value']
)
rule_entry = VertexLinkRuleEntry('ExternalId', '_changed_', rule_constants, [rule_specifiers], 'create')

change_data = {
    'object_type': 'Change',
    'internal_id': '7b4043d1fe270c1ed1f032f8ec31e899',
    'identifier_stem': IdentifierStem.from_raw('#vertex#Change#{\"id_source\": \"MBI\", \"id_type\": \"ChangeLogDetail\", \"id_name\": \"changelogdetail_id\"}#'),
    'object_properties': {
        'detail_id': Decimal('1230'),
        'id_source': 'MBI',
        'changelog_id': Decimal('177'),
        'data_dict_id': Decimal('49'),
        'detail_one': 'e7b0192b71294db66f1ac4e0a9b36bff',
        'detail_one_value': 'e7b0192b71294db66f1ac4e0a9b36bff',
        'detail_two': 'e7b0192b71294db66f1ac4e0a9b36bff'
    },
    'id_value': 1230,
    'id_value_field': 'detail_id',
    'if_missing': None
}

external_id_data = {
    'object_type': 'ExternalId',
    'internal_id': 'ff9ccecb77051747a9ff6cc4169de27d',
    'identifier_stem': IdentifierStem.from_raw('#vertex#ExternalId#{\"id_source\": \"MBI\", \"id_type\": \"Clients\", \"id_name\": \"client_id\"}#'),
    'object_properties': {
        'id_value': Decimal(3889),
        'id_source': 'MBI',
        'id_type': 'Employees',
        'id_name': 'emp_id'
    },
    'id_value': 3889,
    'id_value_field': 'id_value',
    'if_missing': 'create'
}

extracted_data = {
    'source': {
        'id_name': 'changelogdetail_id',
        'id_type': 'ChangeLogDetail',
        'id_source': 'MBI',
        'detail_id': 1230,
        'changelog_id': 177,
        'data_dict_id': 49,
        'detail_one_value': '',
        'detail_one': '',
        'detail_two': '301-537-8676'
    },
    'changed_target': [
        {
            'change_date_utc': datetime.datetime(2014, 7, 29, 14, 44, 44, 367000),
            'client_id': '',
            'clientvisit_id': '',
            'emp_id': 3889,
            'record_id': '',
            'record_type': '',
            'primarykey_name': ''
        }
    ]
}

change_potential_vertex = PotentialVertex.from_json(change_data)

external_id_potential_vertex = PotentialVertex.from_json(external_id_data)

first_identifiable_assimilation_order = AssimilateObjectOrder(
    source_vertex=change_potential_vertex,
    potential_vertex=external_id_potential_vertex,
    rule_entry=rule_entry,
    extracted_data=extracted_data
)
