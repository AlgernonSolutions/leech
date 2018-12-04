from decimal import Decimal
import datetime
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex, MissingObjectProperty
from toll_booth.alg_obj.graph.schemata.rules import VertexLinkRuleEntry, FunctionSpecifier, SharedPropertySpecifier

credible_ws_extractor = 'CredibleWebServiceExtractor'
credible_ws_extractor_function = 'leech-extract-crediblews'
ext_id_identifier_stem = '#vertex#ExternalId#{\"id_source\": \"MBI\", \"id_type\": \"Clients\", \"id_name\": \"client_id\"}#'
change_identifier_stem = '#vertex#Change#{\"id_source\": \"MBI\", \"id_type\": \"ChangeLogDetail\", \"id_name\": \"changelogdetail_id\"}#'
ext_id_id_value = 1941
change_id_value = 1230
change_internal_id = '7b4043d1fe270c1ed1f032f8ec31e899'
ext_id_extracted_data = {
    "source": {"id_value": ext_id_id_value, "id_type": "Clients", "id_name": "client_id", "id_source": "MBI"}}
change_extracted_data = {
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

second_change_id_value = 1458
second_change_extracted_data = {
    "source": {
        "id_name": "changelogdetail_id",
        "id_type": "ChangeLogDetail",
        "id_source": "MBI",
        "detail_id": 1458,
        "changelog_id": 8008,
        "data_dict_id": 521,
        "detail_one_value": "",
        "detail_one": "24",
        "detail_two": "88"},
    "changed_target": [
        {
            "change_date_utc": {"_alg_class": "datetime", "value": 1406872700.68},
            "client_id": 2798,
            "clientvisit_id": "",
            "emp_id": "",
            "record_id": 1164,
            "record_type": "Authorizations",
            "primarykey_name": "auth_id"
        }
    ]
}

change_properties = {'detail_id': Decimal('1230'), 'id_source': 'MBI', 'changelog_id': Decimal('177'),
                     'data_dict_id': Decimal('49'), 'detail_one': 'e7b0192b71294db66f1ac4e0a9b36bff',
                     'detail_one_value': 'e7b0192b71294db66f1ac4e0a9b36bff',
                     'detail_two': 'e7b0192b71294db66f1ac4e0a9b36bff'}
source_vertex = PotentialVertex('Change', change_internal_id, change_properties, change_identifier_stem,
                                change_id_value, 'detail_id')

first_potential_internal_id = 'ff9ccecb77051747a9ff6cc4169de27d'
first_potential_vertex_properties = {'id_value': Decimal('3889'), 'id_source': 'MBI', 'id_type': 'Employees',
                                     'id_name': 'emp_id'}
first_potential_identifier_stem = '#vertex#ExternalId#{"id_source": "MBI", "id_type": "Employees", "id_name": "emp_id"}#'
first_potential_vertex = PotentialVertex('ExternalId', first_potential_internal_id, first_potential_vertex_properties,
                                         first_potential_identifier_stem, 3889, 'id_value')

first_rule_constants = [{'constant_name': 'id_source', 'constant_value': 'source.id_source'}]
first_rule_specifiers = FunctionSpecifier('changed_target', 'derive_change_targeted',
                                          ['id_source', 'id_type', 'id_name', 'id_value'])
first_rule = VertexLinkRuleEntry('ExternalId', '_changed_', first_rule_constants, [first_rule_specifiers], 'create')

second_potential_internal_id = 'c5bf540a3fb43491ad13e446cb9c3757'
second_potential_vertex_properties = {'changelog_id': Decimal('177'), 'change_description': MissingObjectProperty(),
                                      'change_date': MissingObjectProperty(),
                                      'change_date_utc': MissingObjectProperty(), 'id_source': 'MBI'}
second_potential_identifier_stem = ['id_source', 'id_type', 'id_name']
second_potential_vertex = PotentialVertex('ChangeLogEntry', second_potential_internal_id,
                                          second_potential_vertex_properties, second_potential_identifier_stem,
                                          3889, 'id_value')

second_rule_constants = []
second_rule_specifiers = SharedPropertySpecifier('changelog_source', ['id_source', 'changelog_id'])
second_rule = VertexLinkRuleEntry('ChangeLogEntry', '_change_', second_rule_constants, second_rule_specifiers, 'stub',
                                  True)
change_log_identifier_stem = '#vertex#ChangeLog#{"id_source": "MBI", "id_type": "ChangeLog", "id_name": "change_date_utc"}#'
change_log_id_value = 1532974725
change_log_extracted_data = {
    "source": {
        "change_date_utc": datetime.datetime.fromtimestamp(1532974725.0),
        "change_description": None,
        "change_date": datetime.datetime.fromtimestamp(1532956725.0),
        "action": "INSERT CLIENT VISIT WEBFORMS",
        "action_id": 38,
        "id_source": "MBI",
        "id_type": "ChangeLog",
        "id_name": "change_date_utc",
        "done_by": "Samuels, I"
    },
    "changed_target": [
        {
            "id_source": "MBI",
            "id_type": "Clients",
            "id_name": "client_id",
            "id_value": 10165
        },
        {
            "id_source": "MBI",
            "id_type": "ClientVisit",
            "id_name": "clientvisit_id",
            "id_value": 1439730
        }
    ]
}
