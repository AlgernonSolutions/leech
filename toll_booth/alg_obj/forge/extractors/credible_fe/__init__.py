from toll_booth.alg_obj.forge.extractors.abstract_extractor import AbstractVertexDrivenExtractor
from toll_booth.alg_obj.forge.extractors.credible_fe.credible_fe import CredibleFrontEndDriver


class CredibleFrontEndExtractor(AbstractVertexDrivenExtractor):
    @classmethod
    def extract(cls, **kwargs):
        pass

    @classmethod
    def get_monitor_extraction(cls, **kwargs):
        with CredibleFrontEndDriver(kwargs['id_source']) as driver:
            return driver.get_monitor_extraction(kwargs['object_type'])

    @classmethod
    def get_field_value_max_mins(cls, **kwargs):
        identifier_stems = kwargs['identifier_stems']
        max_mins = {}
        with CredibleFrontEndDriver(kwargs['id_source']) as driver:
            for identifier_stem in identifier_stems:
                results = driver.get_field_value_max_min(kwargs['identifier_stem'])
                max_mins[str(identifier_stem)] = results
        return max_mins
