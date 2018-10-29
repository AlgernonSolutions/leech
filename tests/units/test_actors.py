from mock import patch

from tests.steps.actor_setup import patches
from toll_booth.alg_obj.forge.comms.orders import ExtractObjectOrder, TransformObjectOrder, AssimilateObjectOrder
from toll_booth.alg_obj.forge.dentist import Dentist
from toll_booth.alg_obj.forge.lizards import MonitorLizard
from toll_booth.alg_obj.forge.robot_in_disguise import DisguisedRobot
from toll_booth.alg_obj.graph.schemata.entry_property import SchemaPropertyEntry
from toll_booth.alg_obj.graph.schemata.rules import VertexRules
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry, SchemaInternalIdKey, SchemaIdentifierStem

identifier_stem_raw = '#vertex#ExternalId#{\"id_source\": \"MBI\", \"id_type\": \"Clients\", \"id_name\": \"client_id\"}#'
vertex_name = 'ExternalId'
id_value = 1941
vertex_properties = {
    'id_source':  SchemaPropertyEntry('id_source', 'String', False, False),
    'id_type':  SchemaPropertyEntry('id_type', 'String', False, False),
    'id_name':  SchemaPropertyEntry('id_name', 'String', False, False),
    'id_value':  SchemaPropertyEntry('id_value', 'Number', False, True)
}
extraction_properties = {
    'id_source': 'MBI',
    'id_type': 'Employees',
    'id_name': 'emp_id',
    'id_value': 1941,
    'queries': [
        {
            'query_name': 'source',
            'query': "SELECT  {id_name} as id_value, '{object_type}' as id_type, '{id_name}' as id_name, '{id_source}' as id_source FROM {object_type} WHERE {id_name} = {id_value}"
        }
    ]
}
extracted_data={"source": {"id_value": 1941, "id_type": "Clients", "id_name": "client_id", "id_source": "MBI"}}
internal_id_key = SchemaInternalIdKey(['id_source', 'id_type', 'id_value'])
identifier_stem = SchemaIdentifierStem(['id_source', 'id_type', 'id_name'])
schema_entry=SchemaVertexEntry(
    vertex_name='ExternalId',
    vertex_properties=vertex_properties,
    internal_id_key=internal_id_key,
    identifier_stem=identifier_stem,
    indexes=[],
    rules=VertexRules([]),
    extract=[]
)

extraction_order = ExtractObjectOrder(
    identifier_stem=identifier_stem_raw,
    id_value=id_value,
    extraction_function_name="leech-extract-crediblews",
    extraction_properties=extraction_properties,
    schema_entry=schema_entry
)

transform_order = TransformObjectOrder(
    identifier_stem=identifier_stem_raw,
    id_value=1941,
    extracted_data=extracted_data,
    schema_entry=schema_entry
)


class TestActors:
    def test_lizard(self):
        with patch(patches.x_ray_patch_begin), patch(patches.x_ray_patch_end):
            lizard = MonitorLizard(identifier_stem=identifier_stem_raw, sample_size=5)
            lizard.monitor()

    def test_dentist(self):
        dentist = Dentist(extraction_order)
        dentist.extract()

    def test_disguised_robot(self):
        disguised_robot = DisguisedRobot(transform_order)
        disguised_robot.transform()

    def test_borg(self):
        pass
