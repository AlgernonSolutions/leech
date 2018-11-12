from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
from toll_booth.alg_obj.forge.comms.orders import ExtractObjectOrder, LinkObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.forge.comms.stage_manager import StageManager
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry


class MonitorSnake:
    def __init__(self, *, identifier_stem, **kwargs):
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        self._identifier_stem = identifier_stem
        self._leech_driver = LeechDriver(table_name='VdGraphObjects')
        self._extractor_names = self._leech_driver.get_extractor_function_names(identifier_stem)
        self._extraction_profile = self._generate_extraction_profile()
        self._schema_entry = SchemaVertexEntry.get(identifier_stem.object_type)
        self._extraction_queue = kwargs.get('extraction_queue', ForgeQueue.get_for_extraction_queue(**kwargs))
        self._link_queue = kwargs.get('link_queue', ForgeQueue.get_for_link_queue(**kwargs))
        self._sample_size = kwargs.get('sample_size', 1000)

    def monitor(self):
        remote_id_values = self._perform_remote_monitor_extraction()
        local_id_values = self._perform_local_monitor_extraction()
        new_id_values = remote_id_values - local_id_values
        unlinked_id_values = local_id_values - remote_id_values
        consistent_id_values = remote_id_values & local_id_values
        self._send_link_orders(new_id_values, unlinked_id_values)
        self._send_extraction_orders(new_id_values)

    def _send_link_orders(self, new_id_values, unlinked_id_values):
        for id_value in new_id_values:
            link_order = self._generate_link_order(id_value, False)
            self._link_queue.add_order(link_order)
        for id_value in unlinked_id_values:
            unlink_order = self._generate_link_order(id_value, True)
            self._link_queue.add_order(unlink_order)
        self._link_queue.push_orders()

    def _send_extraction_orders(self, new_id_values):
        for id_value in new_id_values:
            extraction_order = self._generate_extraction_order(id_value)
            self._extraction_queue.add_order(extraction_order)
        self._extraction_queue.push_orders()

    def _perform_remote_monitor_extraction(self):
        remote_id_values = StageManager.run_monitoring_extraction(self._extractor_names['monitor_extraction'], self._extraction_profile)
        return set(remote_id_values)

    def _perform_local_monitor_extraction(self):
        local_id_values = self._leech_driver.get_local_id_values(self._identifier_stem)
        return set(local_id_values)

    def _generate_extraction_profile(self):
        extraction_properties = self._identifier_stem.for_extractor
        schema_extraction_properties = self._schema_entry.extract[self._extractor_names['type']]
        extraction_properties.update(schema_extraction_properties.extraction_properties)
        return extraction_properties

    def _generate_extraction_order(self, id_value):
        extractor_name = self._extractor_names['extraction']
        extraction_profile = self._extraction_profile
        return ExtractObjectOrder(
            self._identifier_stem, id_value, extractor_name, extraction_profile, self._schema_entry)

    def _generate_link_order(self, id_value, unlink):
        return LinkObjectOrder(
            self._identifier_stem, id_value, unlink
        )
