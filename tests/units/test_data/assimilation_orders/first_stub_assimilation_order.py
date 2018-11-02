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
        "detail_id": 1482,
        "changelog_id": 8813,
        "data_dict_id": 529,
        "detail_one_value": "40",
        "detail_one": "Nursing Note",
        "detail_two": "Community Support-In"
    },
    "changed_target": [
        {
            "change_date_utc": datetime.fromtimestamp(1406874727.28),
            "client_id": 1542,
            "clientvisit_id": "",
            "emp_id": "",
            "record_id": 1208,
            "record_type": "Authorizations",
            "primarykey_name": "auth_id"}
    ]
}

stub_changelog_entry_data = {
    'object_type': 'ChangeLogEntry',
    'object_properties': {
        'changelog_id': Decimal(8813),
        'change_description': MissingObjectProperty(),
        'change_date': MissingObjectProperty(),
        'change_date_utc': MissingObjectProperty(),
        'id_source': 'MBI'
    },
    'identifier_stem': ['id_source', 'id_type', 'id_name'],
    'internal_id': '190237dff8ed7eee76a1cdd467af5837',
    'id_value_field': 'changelog_id',
    'id_value': 8813,
    'if_missing': 'stub'
}

change_data = {
    'object_type': 'Change',
    'identifier_stem': IdentifierStem.from_raw('"#vertex#Change#{\"id_source\": \"MBI\", \"id_type\": \"ChangeLogDetail\", \"id_name\": \"changelogdetail_id\"}#"'),
    'object_properties': {
        'detail_id': Decimal(1482),
        'id_source': 'MBI',
        'changelog_id': Decimal(8813),
        'data_dict_id': Decimal(529),
        'detail_one': "4a7e6efa625a9e63eaa86396dfbde1b7",
        "detail_one_value": "4a7e6efa625a9e63eaa86396dfbde1b7",
        "detail_two": "4a7e6efa625a9e63eaa86396dfbde1b7",
    },
    'internal_id': 'b89431ac59174cdc38f3ceee61a4cdc9',
    'id_value': Decimal(1482),
    'id_value_field': 'detail_id',
    'if_missing': None
}

first_stub_assimilate_order = AssimilateObjectOrder(
    source_vertex=PotentialVertex.from_json(change_data),
    potential_vertex=PotentialVertex.from_json(stub_changelog_entry_data),
    rule_entry=rule_entry,
    extracted_data=extracted_data
)
