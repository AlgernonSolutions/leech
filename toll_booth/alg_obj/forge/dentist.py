from toll_booth.alg_obj.forge.comms.orders import TransformObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.forge.extractors.finder import ExtractorFinder
from toll_booth.alg_obj.graph.troubles import InvalidExtractionMultipleSourceException


class Dentist:
    def __init__(self, metal_order, **kwargs):
        self._extraction_order = metal_order
        self._extraction_source_name = metal_order.extraction_source
        self._extraction_properties = metal_order.extraction_properties
        self._schema_entry = metal_order.schema_entry
        self._transform_queue = kwargs.get('transform_queue', ForgeQueue.get_for_transform_queue(**kwargs))

    def extract(self):
        extractor = ExtractorFinder.find_by_name(self._extraction_source_name)
        extracted_data = extractor.extract(extraction_order=self._extraction_order)
        source_data = extracted_data['source']
        if len(source_data) > 1:
            raise InvalidExtractionMultipleSourceException(self._extraction_source_name, self._extraction_order)
        for entry in source_data:
            if not entry:
                return
            extracted_data['source'] = entry
            break
        transform_order = TransformObjectOrder(extracted_data, self._schema_entry)
        self._transform_queue.add_order(transform_order)
        self._transform_queue.push_orders()
