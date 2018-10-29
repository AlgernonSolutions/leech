from toll_booth.alg_obj.forge.extractors.abstract_extractor import AbstractExtractor
from toll_booth.alg_obj.forge.extractors.credible_ws.credible_ws import CredibleDriver


class CredibleWebServiceExtractor(AbstractExtractor):
    @classmethod
    def extract(cls, **kwargs):
        extraction_properties = kwargs.copy()
        id_type = kwargs['id_type']
        if id_type:
            extraction_properties['object_type'] = id_type
        driver = CredibleDriver(kwargs['id_source'])
        extracted_data = {}
        for query_entry in kwargs['queries']:
            query = query_entry['query'].format(**extraction_properties)
            results = driver.run(query)
            query_name = query_entry['query_name']
            extracted_data[query_name] = results
        return extracted_data

    @classmethod
    def get_current_remote_max_min_id(cls, **kwargs):
        id_type = kwargs['id_type']
        object_type = kwargs['object_type']
        if id_type:
            object_type = id_type
        credible_driver = CredibleDriver(kwargs['id_source'])
        remote_max_min = credible_driver.get_remote_max_min(object_type, kwargs['id_name'])
        return {'max': remote_max_min.max_id, 'min': remote_max_min.min_id}
