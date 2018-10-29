import pytest
import json

from toll_booth.alg_obj.forge.comms.orders import ExtractObjectOrder
from toll_booth.alg_obj.serializers import AlgDecoder

event = {
    'task_name': 'extract',
    'task_args': '{"metal_order": {"_alg_class": "<class \'toll_booth.alg_obj.forge.comms.orders.ExtractObjectOrder\'>", "value": {"identifier_stem": "#vertex#ExternalId#{\\"id_source\\": \\"MBI\\", \\"id_type\\": \\"Employees\\", \\"id_name\\": \\"emp_id\\"}#", "id_value": 3962, "schema_entry": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.schema_entry.SchemaVertexEntry\'>", "value": {"_entry_name": "ExternalId", "_internal_id_key": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.schema_entry.SchemaInternalIdKey\'>", "value": {"_field_names": ["id_source", "id_type", "id_value"]}}, "_entry_properties": {"id_value": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.entry_property.SchemaPropertyEntry\'>", "value": {"_property_name": "id_value", "_property_data_type": "Number", "_sensitive": false, "_is_id_value": true}}, "id_source": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.entry_property.SchemaPropertyEntry\'>", "value": {"_property_name": "id_source", "_property_data_type": "String", "_sensitive": false, "_is_id_value": false}}, "id_type": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.entry_property.SchemaPropertyEntry\'>", "value": {"_property_name": "id_type", "_property_data_type": "String", "_sensitive": false, "_is_id_value": false}}, "id_name": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.entry_property.SchemaPropertyEntry\'>", "value": {"_property_name": "id_name", "_property_data_type": "String", "_sensitive": false, "_is_id_value": false}}}, "_indexes": {"external_id_by_internal_id": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.indexes.UniqueIndexEntry\'>", "value": {"_index_name": "external_id_by_internal_id", "_index_type": "unique", "_index_properties": {"key": ["internal_id"]}, "_key": ["internal_id"]}}, "external_id_internal_id_range_by_source": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.indexes.SortedSetIndexEntry\'>", "value": {"_index_name": "external_id_internal_id_range_by_source", "_index_type": "sorted_set", "_index_properties": {"score": "0", "key": ["id_source", "id_type", "id_name"]}, "_score": "0", "_key": ["id_source", "id_type", "id_name"]}}, "external_id_id_value_range_by_source": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.indexes.SortedSetIndexEntry\'>", "value": {"_index_name": "external_id_id_value_range_by_source", "_index_type": "sorted_set", "_index_properties": {"score": "id_value", "key": ["id_source", "id_type", "id_name"]}, "_score": "id_value", "_key": ["id_source", "id_type", "id_name"]}}}, "_rules": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.rules.VertexRules\'>", "value": {"_linking_rules": []}}, "_vertex_properties": {"id_value": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.entry_property.SchemaPropertyEntry\'>", "value": {"_property_name": "id_value", "_property_data_type": "Number", "_sensitive": false, "_is_id_value": true}}, "id_source": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.entry_property.SchemaPropertyEntry\'>", "value": {"_property_name": "id_source", "_property_data_type": "String", "_sensitive": false, "_is_id_value": false}}, "id_type": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.entry_property.SchemaPropertyEntry\'>", "value": {"_property_name": "id_type", "_property_data_type": "String", "_sensitive": false, "_is_id_value": false}}, "id_name": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.entry_property.SchemaPropertyEntry\'>", "value": {"_property_name": "id_name", "_property_data_type": "String", "_sensitive": false, "_is_id_value": false}}}, "_vertex_name": "ExternalId", "_extract": {"CredibleWebServiceExtractor": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.schema_entry.ExtractionInstruction\'>", "value": {"_extraction_source": "CredibleWebServiceExtractor", "_extraction_properties": {"queries": [{"query_name": "source", "query": "SELECT {id_name} as id_value, \'{object_type}\' as id_type, \'{id_name}\' as id_name, \'{id_source}\' as id_source FROM {object_type} WHERE {id_name} = {id_value}"}]}}}, "CredibleFrontEndExtractor": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.schema_entry.ExtractionInstruction\'>", "value": {"_extraction_source": "CredibleFrontEndExtractor", "_extraction_properties": {"extraction_url": "crediblebh.com"}}}, "GenericExtractor": {"_alg_class": "<class \'toll_booth.alg_obj.graph.schemata.schema_entry.ExtractionInstruction\'>", "value": {"_extraction_source": "GenericExtractor", "_extraction_properties": {}}}}, "_identifier_stem": ["id_source", "self.vertex_name", "id_type"]}}, "extraction_function_name": "leech-extract-crediblews", "extraction_properties": {"id_source": "MBI", "id_type": "Employees", "id_name": "emp_id", "graph_type": "vertex", "object_type": "ExternalId", "id_value": 4133}, "action_name": "extract"}}}'}


class TestExtractor:
    def test_extract_order_generation(self):
        extract_order = json.loads(event['task_args'], cls=AlgDecoder)
        extract_order = ExtractObjectOrder.from_json(extract_order['metal_order'])
        assert isinstance(extract_order, ExtractObjectOrder)


TestExtractor().test_extract_order_generation()