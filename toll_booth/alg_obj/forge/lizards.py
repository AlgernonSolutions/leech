import logging

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.aws_obj.lockbox import IndexDriver, FuseLighter
from toll_booth.alg_obj.forge.comms.orders import ExtractObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.forge.extractors.finder import ExtractorFinder
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry


class MonitorLizard:
    @xray_recorder.capture('lizard_init')
    def __init__(self, *, object_identifier, index_name, extraction_source_name, **kwargs):
        self._schema_entry = SchemaVertexEntry.get(object_identifier['object_type'])
        self._extraction_source_name = extraction_source_name
        # TODO wtf why are we doing this
        extraction_properties = self._schema_entry.extraction[extraction_source_name].extraction_properties
        extraction_properties.update(object_identifier)
        self._extraction_properties = extraction_properties
        self._index_name = index_name
        self._index_driver = IndexDriver()
        self._extraction_queue = kwargs.get('extraction_queue', ForgeQueue.get_for_extraction_queue(**kwargs))
        self._fuse_lighter = FuseLighter(object_identifier, **kwargs)
        self._sample_size = kwargs.get('sample_size', 1000)

    @xray_recorder.capture('lizard_monitor')
    def monitor(self):
        max_local_id = self._get_current_local_max_id()
        max_remote_id = self._get_current_remote_max_id()
        missing_id_values = self._find_missing_id_values(max_remote_id, max_local_id)
        extraction_orders = []
        working_ids = []
        for missing_id_value in missing_id_values:
            if self._check_if_id_working(missing_id_value) is True:
                working_ids.append(missing_id_value)
                continue
            self._mark_id_as_working(missing_id_value)
            extraction_orders.append(self._generate_extraction_order(missing_id_value))
        logging.info(f'completed monitoring for {self._schema_entry.entry_name}, '
                     f'remote: {max_remote_id}, local: {max_local_id}, '
                     f'{len(self._extraction_queue)} extraction orders to be sent, '
                     f'{len(working_ids)} values are already being processed')
        self._send_extraction_orders(extraction_orders)

    @xray_recorder.capture('lizard_send_extraction_order')
    def _send_extraction_orders(self, extraction_orders):
        self._extraction_queue.add_orders(extraction_orders)
        self._extraction_queue.push_orders()

    @xray_recorder.capture('lizard_generate_extraction_order')
    def _generate_extraction_order(self, missing_id_value):
        return ExtractObjectOrder(
            missing_id_value, self._extraction_source_name, self._extraction_properties, self._schema_entry)

    # TODO Why is this like this??? Can it be a range?
    @xray_recorder.capture('lizard_find_missing')
    def _find_missing_id_values(self, max_remote_id, max_local_id):
        missing_values = []
        while max_remote_id >= max_local_id and len(missing_values) <= self._sample_size:
            missing_values.append(max_remote_id)
            max_remote_id -= 1
        return missing_values

    @xray_recorder.capture('lizard_get_remote_max')
    def _get_current_remote_max_id(self):
        extractor = ExtractorFinder.find_by_name(self._extraction_source_name)
        max_id_value = extractor.get_current_remote_max_id(**self._extraction_properties)
        return max_id_value

    @xray_recorder.capture('lizard_get_local_max')
    def _get_current_local_max_id(self):
        local_max_min = self._index_driver.query_index_max_min(self._index_name)
        if not local_max_min:
            return 0
        return local_max_min['max'][1]

    @xray_recorder.capture('lizard_check_if_working')
    def _check_if_id_working(self, missing_id_value):
        return self._fuse_lighter.check_if_id_working(missing_id_value)

    @xray_recorder.capture('lizard_mark_as_working')
    def _mark_id_as_working(self, missing_id_value):
        self._fuse_lighter.mark_id_as_working(missing_id_value)
