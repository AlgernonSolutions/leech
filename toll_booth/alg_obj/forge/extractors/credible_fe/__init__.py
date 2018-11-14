import re
from decimal import Decimal, InvalidOperation

from toll_booth.alg_obj.forge.extractors.abstract_extractor import AbstractVertexDrivenExtractor
from toll_booth.alg_obj.forge.extractors.credible_fe.credible_fe import CredibleFrontEndDriver


class CredibleFrontEndExtractor(AbstractVertexDrivenExtractor):
    @classmethod
    def extract(cls, **kwargs):
        extracted_data = {}
        identifiers = kwargs['identifier_stems']
        id_source = kwargs['id_source']
        with CredibleFrontEndDriver(id_source) as driver:
            for identifier in identifiers:
                identifier_stem = identifier['identifier_stem']
                id_value = identifier['id_value']
                object_type = identifier_stem.object_type
                if object_type == 'ChangeLog':
                    mapping = kwargs['mapping']
                    id_source_mapping = mapping.get(id_source, mapping['default'])
                    object_mapping = id_source_mapping[identifier_stem.get('id_type')]
                    source_extraction = driver.get_change_logs(identifier_stem, id_value)
                    change_detail_extraction = driver.get_change_details(identifier_stem, id_value)
                    formatted_extraction = cls._format_extracted_data(
                        identifier_stem, source_extraction, object_mapping, change_detail_extraction, 'change_details')
                    extracted_data.update(formatted_extraction)
                    continue
                raise NotImplementedError(
                    'do not know how to extract object %s through the Credible Front End' % object_type)
        return extracted_data

    @classmethod
    def get_monitor_extraction(cls, **kwargs):
        with CredibleFrontEndDriver(kwargs['id_source']) as driver:
            return driver.get_monitor_extraction(kwargs['object_type'])

    @classmethod
    def get_field_values(cls, **kwargs):
        identifier_stems = kwargs['identifier_stems']
        id_sources = set(x.get('id_source') for x in identifier_stems)
        if len(id_sources) > 1:
            raise NotImplementedError(
                'attempted to extract field_value max/mins for identifier_stems: %s, but the request contained '
                'multiple id_sources, currently this feature is not supported' % identifier_stems
            )
        field_values = {}
        for id_source in id_sources:
            with CredibleFrontEndDriver(id_source) as driver:
                for identifier_stem in identifier_stems:
                    results = driver.get_change_logs(kwargs['identifier_stem'])
                    field_values[str(identifier_stem)] = results
            return field_values

    @classmethod
    def _format_extracted_data(cls, identifier_stem, extracted_data, mapping, side_data=None, side_data_name='side_data'):
        formatted_data = {}
        for key_value, extracted_row in extracted_data.items():
            formatted_row = {}
            for field_name, field_value in extracted_row.items():
                if field_name in mapping:
                    row_mapping = mapping[field_name]
                    field_name = row_mapping['name']
                    mutation = row_mapping['mutation']
                    if mutation and field_value:
                        field_value = getattr(cls, '_'+mutation)(field_value)
                    formatted_row[field_name] = field_value
            utc_time = formatted_row['change_date_utc']
            finished_row = {'source': formatted_row}
            if side_data:
                finished_row[side_data_name] = side_data.get(utc_time, {})
            data_key = (str(identifier_stem), utc_time.timestamp())
            formatted_data[data_key] = finished_row
        return formatted_data

    @classmethod
    def _split_client_name(cls, field_value):
        id_pattern = '\((?P<client_id>[\d]+)\)'
        matches = re.compile(id_pattern).search(field_value)
        if not matches:
            print()
        return matches.group('client_id')

    @classmethod
    def _split_record_id(cls, field_value):
        id_inside_pattern = '\((?P<record_id>[\d]+)\)'
        id_outside_pattern = '\((?P<record_id>[\S]+)\)'
        inside_matches = re.compile(id_inside_pattern).search(field_value)
        outside_matches = re.compile(id_outside_pattern).search(field_value)
        if inside_matches:
            record_id = inside_matches.group('record_id')
            record_bit = f'({record_id})'
            id_type = field_value.replace(record_bit, '')
            if record_id:
                record_id = Decimal(record_id)
            return {'id_value': record_id, 'id_type': id_type}
        if outside_matches:
            id_type = outside_matches.group('record_id')
            id_type_bit = f'({id_type})'
            record_id = field_value.replace(id_type_bit, '')
            if record_id:
                record_id = Decimal(record_id)
            return {'id_value': record_id, 'id_type': id_type}

        try:
            return {'id_value': Decimal(field_value)}
        except InvalidOperation:
            print()
        raise NotImplementedError('was instructed to parse out a record id from %s but it looks a bit wonky, sorry boss' % field_value)
