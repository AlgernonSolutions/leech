from toll_booth.alg_obj.aws.sapper.dynamo_driver import LeechDriver
from toll_booth.alg_obj.forge.comms.orders import TransformObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.forge.comms.stage_manager import StageManager
from toll_booth.alg_obj.graph.troubles import InvalidExtractionMultipleSourceException


class Dentist:
    def __init__(self, metal_order, **kwargs):
        self._extraction_order = metal_order
        self._extraction_function_name = metal_order.extraction_function_name
        self._extraction_properties = metal_order.extraction_properties
        self._schema_entry = metal_order.schema_entry
        self._dynamo_driver = LeechDriver()
        self._transform_queue = kwargs.get('transform_queue', ForgeQueue.get_for_transform_queue(**kwargs))

    def extract(self):
        extracted_data = StageManager.run_extraction(
            self._extraction_function_name, self._extraction_properties)
        source_data = extracted_data['source']
        if len(source_data) > 1:
            raise InvalidExtractionMultipleSourceException(self._extraction_function_name, self._extraction_order)
        if not source_data:
            return self._mark_object_blank()
        for entry in source_data:
            if not entry:
                return self._mark_object_blank()
            extracted_data['source'] = entry
            break
        transform_order = TransformObjectOrder(
            self._extraction_order.identifier_stem,
            self._extraction_order.id_value,
            extracted_data,
            self._schema_entry
        )
        self._transform_queue.add_order(transform_order)
        self._dynamo_driver.set_extraction_results(
            extracted_data,
            identifier_stem=self._extraction_order.identifier_stem,
            id_value=self._extraction_order.id_value)
        self._transform_queue.push_orders()

    def _mark_object_blank(self):
        return self._dynamo_driver.mark_object_as_blank(
            identifier_stem=self._extraction_order.identifier_stem,
            id_value=self._extraction_order.id_value
        )