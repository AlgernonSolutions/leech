import logging

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver, EmptyIndexException
from toll_booth.alg_obj.forge.comms.orders import ExtractObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.forge.comms.stage_manager import StageManager
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry


class MonitorLizard:
    @xray_recorder.capture('lizard_init')
    def __init__(self, *, identifier_stem, **kwargs):
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        self._identifier_stem = identifier_stem
        self._object_type = identifier_stem.object_type
        self._schema_entry = SchemaVertexEntry.retrieve(self._object_type)
        self._leech_driver = LeechDriver()
        self._extractor_setup = self._leech_driver.get_extractor_setup(identifier_stem)
        self._extraction_profile = self._generate_extraction_profile()
        self._extraction_queue = kwargs.get('extraction_queue', ForgeQueue.get_for_extraction_queue(**kwargs))
        self._sample_size = kwargs.get('sample_size', 1000)

    @xray_recorder.capture('lizard_monitor')
    def monitor(self):
        remote_ids = self._get_current_remote_max_min_id()
        try:
            max_local_id = self._get_current_local_max_id()
            max_remote_id = remote_ids['max']
        except EmptyIndexException:
            max_local_id = remote_ids['min']
            max_remote_id = remote_ids['max']
        extraction_orders = []
        if max_remote_id > max_local_id:
            id_range = range(max_local_id+1, max_remote_id+1)
            id_range = id_range[:self._sample_size]
            already_working, not_working = self._leech_driver.mark_ids_as_working(
                id_range, identifier_stem=self._identifier_stem)
            for id_value in not_working:
                extraction_orders.append(self._generate_extraction_order(id_value))
            orders_to_be_sent = len(self._extraction_queue)
            self._send_extraction_orders(extraction_orders)
            logging.info(
                f'completed monitoring for {self._schema_entry.entry_name}, '
                f'remote: {max_remote_id}, local: {max_local_id}, '
                f'{orders_to_be_sent} extraction orders to be sent, '
                f'{len(already_working)} values are already being processed')
            return
        logging.info(
            f'completed monitoring for {self._schema_entry.entry_name}, '
            f'remote: {max_remote_id}, local: {max_local_id}, '
            f'no new objects found')

    @xray_recorder.capture('lizard_send_extraction_order')
    def _send_extraction_orders(self, extraction_orders):
        self._extraction_queue.add_orders(extraction_orders)
        self._extraction_queue.push_orders()

    @xray_recorder.capture('lizard_generate_extraction_order')
    def _generate_extraction_order(self, missing_id_value):
        extractor_name = self._extractor_setup['extraction']
        extraction_profile = self._extraction_profile
        return ExtractObjectOrder(
            self._identifier_stem, missing_id_value, extractor_name, extraction_profile, self._schema_entry)

    @xray_recorder.capture('lizard_generate_extraction_properties')
    def _generate_extraction_profile(self):
        extraction_properties = self._identifier_stem.for_extractor
        schema_extraction_properties = self._schema_entry.extract[self._extractor_setup['type']]
        extraction_properties.update(schema_extraction_properties.extraction_properties)
        return extraction_properties

    @xray_recorder.capture('lizard_get_remote_max')
    def _get_current_remote_max_min_id(self):
        id_values = StageManager.run_index_query(self._extractor_setup['index_query'], self._extraction_profile)
        return id_values

    @xray_recorder.capture('lizard_get_local_max')
    def _get_current_local_max_id(self):
        return self._leech_driver.query_index_value_max(self._identifier_stem)

