from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver


class FipExtractors:
    @classmethod
    def get_fip_extractor(cls, **kwargs):
        from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
        identifier_stem = IdentifierStem.from_raw(kwargs['identifier_stem'])
        id_type = identifier_stem.get('id_type')
        fn_name = f'_fip_extract_{id_type}'
        return getattr(cls, fn_name)

    @classmethod
    def _fip_extract_providers(cls, **kwargs):
        from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
        identifier_stem = IdentifierStem.from_raw(kwargs['identifier_stem'])
        id_source = identifier_stem.get('id_source')
        provider_data = []
        with CredibleFrontEndDriver(id_source) as driver:
            is_bulk = kwargs.get('is_bulk', False)
            if is_bulk:
                id_values = kwargs['id_values']
        return

    @classmethod
    def _fip_extract_patients(cls, **kwargs):
        return

    @classmethod
    def _fip_extract_encounters(cls, **kwargs):
        return
