from datetime import datetime
from decimal import Decimal

from toll_booth.alg_obj.forge.comms.orders import AssimilateObjectOrder
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem, MissingObjectProperty, PotentialVertex
from toll_booth.alg_obj.graph.schemata.rules import SharedPropertySpecifier, VertexLinkRuleEntry

source_vertex_data = {
    "object_type": "Change",
    "object_properties": {
        "detail_id": Decimal(1247),
        "id_source": "MBI",
        "changelog_id":  Decimal(654),
        "data_dict_id": None,
        "detail_one": None,
        "detail_one_value": None,
        "detail_two": None},
    "internal_id": "0d8126460981670a0ce6b0f406ebf760",
    "identifier_stem": IdentifierStem('vertex', 'Change', {"id_source": "MBI", "id_type": "ChangeLogDetail", "id_name": "changelogdetail_id"}),
    "id_value": Decimal(1247),
    "id_value_field": "detail_id",
    "graph_as_stub": False
}

potential_vertex_data = {
    "object_type": "ChangeLogEntry",
    "object_properties": {
        "changelog_id": Decimal(654),
        "change_description": MissingObjectProperty(),
        "change_date": MissingObjectProperty(),
        "change_date_utc": MissingObjectProperty(),
        "id_source": "MBI"
    },
    "internal_id": "8312e2bb34f4ea6b6312c25ab10f0738",
    "identifier_stem": ["id_source", "id_type", "id_name"],
    "id_value": Decimal(654),
    "id_value_field": "changelog_id",
    "graph_as_stub": False
}

rule_entry_data = {
    "target_type": "ChangeLogEntry",
    "edge_type": "_change_",
    "target_constants": [],
    "target_specifiers": [
        SharedPropertySpecifier('changelog_source', ['id_source', 'changelog_id'])
    ],
    "if_absent": "stub",
    "inbound": True,
    "function_name": None
}

extracted_data = {
        "source": {
            "id_name": "changelogdetail_id",
            "id_type": "ChangeLogDetail",
            "id_source": "MBI",
            "detail_id": 1247,
            "changelog_id": 654,
            "data_dict_id": "",
            "detail_one_value": "",
            "detail_one": "",
            "detail_two": ""
        },
        "changed_target": [
            {
                "change_date_utc": datetime.fromtimestamp(1406686426.51),
                "client_id": "",
                "clientvisit_id": "",
                "emp_id": 3796,
                "record_id": 30,
                "record_type": "ExportBuilder",
                "primarykey_name": "exportbuilder_id"
            }
        ]
    }

change_to_stub_changelog_assimilation_order = AssimilateObjectOrder(
    PotentialVertex.from_json(source_vertex_data),
    PotentialVertex.from_json(potential_vertex_data),
    VertexLinkRuleEntry.from_json(rule_entry_data),
    extracted_data
)
