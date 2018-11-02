from datetime import datetime
from decimal import Decimal

from toll_booth.alg_obj.forge.comms.orders import AssimilateObjectOrder
from toll_booth.alg_obj.graph.ogm.regulators import MissingObjectProperty, IdentifierStem, PotentialVertex
from toll_booth.alg_obj.graph.schemata.rules import VertexLinkRuleEntry, SharedPropertySpecifier

rule_entry = VertexLinkRuleEntry(
    'ChangeLogEntry', '_change_', [],
    SharedPropertySpecifier('changelog_source', ['id_source', 'changelog_id']),
    'stub', True
)

extracted_data = {
    "source": {
        "id_name": "changelogdetail_id",
        "id_type": "ChangeLogDetail",
        "id_source": "MBI",
        "detail_id": 1230,
        "changelog_id": 177,
        "data_dict_id": 49,
        "detail_one_value": "",
        "detail_one": "",
        "detail_two": "301-537-8676"
    },
    "changed_target": [
        {
            "change_date_utc": datetime(2014, 7, 29, 14, 44, 44, 367000),
            "client_id": '',
            "clientvisit_id": '',
            "emp_id": 3889,
            "record_id": '',
            "record_type": '',
            "primarykey_name": ''}
    ]
}

stub_changelog_entry_data = {
    'object_type': 'ChangeLogEntry',
    'object_properties': {
        'changelog_id': Decimal(177),
        'change_description': MissingObjectProperty(),
        'change_date': MissingObjectProperty(),
        'change_date_utc': MissingObjectProperty(),
        'id_source': 'MBI'
    },
    'identifier_stem': ['id_source', 'id_type', 'id_name'],
    'internal_id': 'c5bf540a3fb43491ad13e446cb9c3757',
    'id_value_field': 'changelog_id',
    'id_value': 177,
    'if_missing': 'stub'
}

change_data = {
    'object_type': 'Change',
    'identifier_stem': IdentifierStem.from_raw('"#vertex#Change#{\"id_source\": \"MBI\", \"id_type\": \"ChangeLogDetail\", \"id_name\": \"changelogdetail_id\"}#"'),
    'object_properties': {
        'detail_id': Decimal(1230),
        'id_source': 'MBI',
        'changelog_id': Decimal(177),
        'data_dict_id': Decimal(49),
        'detail_one': 'e7b0192b71294db66f1ac4e0a9b36bff',
        'detail_one_value': 'e7b0192b71294db66f1ac4e0a9b36bff',
        'detail_two': 'e7b0192b71294db66f1ac4e0a9b36bff'},
    'internal_id': '7b4043d1fe270c1ed1f032f8ec31e899',
    'id_value': Decimal(1230),
    'id_value_field': 'detail_id',
    'if_missing': None
}

second_stub_assimilate_order = AssimilateObjectOrder(
    source_vertex=PotentialVertex.from_json(change_data),
    potential_vertex=PotentialVertex.from_json(stub_changelog_entry_data),
    rule_entry=rule_entry,
    extracted_data=extracted_data
)

