import logging

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.aws_obj.dynamo_driver import DynamoDriver
from toll_booth.alg_obj.forge.comms.orders import ExtractObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.forge.extractors.finder import ExtractorFinder
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry


class MonitorLizard:
    @xray_recorder.capture('lizard_init')
    def __init__(self, *, object_identifier, extraction_source_name, **kwargs):
        self._object_type = object_identifier['object_type']
        self._schema_entry = SchemaVertexEntry.get(self._object_type)
        self._extraction_source_name = extraction_source_name
        # TODO wtf why are we doing this
        extraction_properties = self._schema_entry.extraction[extraction_source_name].extraction_properties
        extraction_properties.update(object_identifier)
        self._extraction_properties = extraction_properties
        self._object_identifier = object_identifier
        self._working_id_stem = self.get_working_id_stem()
        self._dynamo_driver = DynamoDriver()
        self._extraction_queue = kwargs.get('extraction_queue', ForgeQueue.get_for_extraction_queue(**kwargs))
        self._sample_size = kwargs.get('sample_size', 1000)

    @xray_recorder.capture('lizard_monitor')
    def monitor(self):
        max_local_id = self._get_current_local_max_id()
        max_remote_id = self._get_current_remote_max_id()
        extraction_orders = []
        if max_remote_id > max_local_id:
            already_working, not_working = self._dynamo_driver.mark_ids_as_working(
                self._working_id_stem, range(max_local_id+1, max_remote_id+1), self._object_type)
            for id_value in not_working:
                extraction_orders.append(self._generate_extraction_order(id_value))
            self._send_extraction_orders(extraction_orders)
            logging.info(
                f'completed monitoring for {self._schema_entry.entry_name}, '
                f'remote: {max_remote_id}, local: {max_local_id}, '
                f'{len(self._extraction_queue)} extraction orders to be sent, '
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
        return ExtractObjectOrder(
            self._working_id_stem, missing_id_value, self._extraction_source_name,
            self._extraction_properties, self._schema_entry
        )

    @xray_recorder.capture('lizard_get_remote_max')
    def _get_current_remote_max_id(self):
        extractor = ExtractorFinder.find_by_name(self._extraction_source_name)
        max_id_value = extractor.get_current_remote_max_id(**self._extraction_properties)
        return max_id_value

    @xray_recorder.capture('lizard_get_local_max')
    def _get_current_local_max_id(self):
        return self._dynamo_driver.query_index_max(self._working_id_stem)

    def get_working_id_stem(self):
        identifier = [x for x in self._object_identifier.values()]
        identifier.sort()
        return '.'.join(identifier)
