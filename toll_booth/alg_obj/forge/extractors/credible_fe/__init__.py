import re
from decimal import Decimal, InvalidOperation

from toll_booth.alg_obj.forge.extractors.abstract_extractor import AbstractVertexDrivenExtractor
from toll_booth.alg_obj.forge.extractors.credible_fe.credible_fe import CredibleFrontEndDriver
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


class CredibleFrontEndExtractor(AbstractVertexDrivenExtractor):
    @classmethod
    def extract(cls, **kwargs):
        identifiers = kwargs.get('identifier_stems', None)
        identifier = kwargs.get('identifier', None)
        if identifiers and identifier:
            raise RuntimeError('cannot run extraction for both a set of identifier stems, and a single stem')
        if identifiers:
            return cls._run_multi_extract(**kwargs)
        if identifier:
            return cls._run_single_extract(**kwargs)

    @classmethod
    def _run_single_extract(cls, identifier, **kwargs):
        extracted_data = {}
        id_source = kwargs['id_source']
        id_value = identifier['id_value']
        identifier_stem = identifier['identifier_stem']
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        kwargs['identifier_stem'] = identifier_stem
        object_type = identifier_stem.object_type
        with CredibleFrontEndDriver(id_source) as driver:
            if object_type == 'ExternalId':
                source_extraction = driver.get_ext_id(identifier_stem)
            if object_type == 'ChangeLog':
                local_max_values = identifier['local_max_values']
                return cls._extract_change_logs(driver, id_value, local_max_values, **kwargs)

    @classmethod
    def _run_multi_extract(cls, id_source, **kwargs):
        extracted_data = {}
        identifier_stems = kwargs['identifier_stems']
        with CredibleFrontEndDriver(id_source) as driver:
            for identifier in identifier_stems:
                identifier_stem = identifier['identifier_stem']
                id_value = identifier['id_value']
                object_type = identifier_stem.object_type
                if object_type == 'ChangeLog':
                    extracted_data.update(
                        cls._extract_change_logs(driver, id_source, identifier_stem, id_value, **kwargs))
                    continue
                if object_type == 'ExternalId':
                    mapping = kwargs['mapping']
                    id_source_mapping = mapping.get(id_source, mapping['default'])
                    object_mapping = id_source_mapping[identifier_stem.get('id_type')]
                    source_extraction = driver.get_ext_id(id)
                raise NotImplementedError(
                    'do not know how to extract object %s through the Credible Front End' % object_type)
        return extracted_data

    @classmethod
    def _extract_change_logs(cls, driver, id_value, local_max_values, **kwargs):
        change_logs = []
        identifier_stem = kwargs['identifier_stem']
        driving_stem = IdentifierStem.from_raw(identifier_stem.get('identifier_stem'))
        driving_id_type = driving_stem.get('id_type')
        driving_id_name = driving_stem.get('id_name')
        id_source = kwargs['id_source']
        mapping = kwargs['mapping']
        id_source_mapping = mapping.get(id_source, mapping['default'])
        object_mapping = id_source_mapping[driving_id_type]
        for category_id, local_max_value in local_max_values.items():
            extraction_args = {
                'driving_id_type': driving_id_type,
                'driving_id_name': driving_id_name,
                'driving_id_value': id_value,
                'category_id': category_id,
                'local_max_value': local_max_value
            }
            source_extraction = driver.get_change_logs(**extraction_args)
            # change_detail_extraction = driver.get_change_details(**extraction_args)
            emp_ids = driver.get_emp_ids(**extraction_args)
            # for change_date, entry in source_extraction.items():
            #   entry['User'] = emp_ids[change_date]
            formatted_extraction = cls._format_change_log_data(
                identifier_stem, source_extraction, object_mapping=object_mapping, driver=driver
            )
            change_logs.extend(formatted_extraction)

    @classmethod
    def get_monitor_extraction(cls, **kwargs):
        with CredibleFrontEndDriver(kwargs['id_source']) as driver:
            return driver.get_monitor_extraction(**kwargs)

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
    def _format_change_log_data(cls, identifier_stem, extracted_data, **kwargs):
        formatted_data = {}
        mapping = kwargs['object_mapping']
        for key_value, extracted_row in extracted_data.items():
            formatted_row = {}
            for field_name, field_value in extracted_row.items():
                if field_name in mapping:
                    row_mapping = mapping[field_name]
                    field_name = row_mapping['name']
                    mutation = row_mapping['mutation']
                    if mutation and field_value:
                        field_value = getattr(cls, '_'+mutation)(field_value, **kwargs)
                    formatted_row[field_name] = field_value
            utc_time = formatted_row['change_date_utc']
            finished_row = {'source': formatted_row}
            if kwargs.get('side_data', None):
                finished_row[kwargs.get('side_data_name', 'side_data')] = kwargs['side_data'].get(utc_time, [{}])
            data_key = (str(identifier_stem), utc_time.timestamp())
            formatted_data[data_key] = finished_row
        return formatted_data

    @classmethod
    def _get_client_id(cls, field_value, **kwargs):
        client_id, client_name = cls._split_entry(field_value)
        return client_id

    @classmethod
    def _split_record_id(cls, field_value, **kwargs):
        record_id, record_type = cls._split_entry(field_value)
        return {'record_id': record_id, 'record_type': record_type}

    @classmethod
    def _split_entry(cls, field_value):
        non_numeric_inside = re.compile('(?P<outside>[\w\s]+?)\s*\((?P<inside>(?=[a-zA-Z\s])[\w\s\d]+)\)')
        numeric_inside = re.compile('(?P<outside>[\w\s]+?)\s*\((?P<inside>[\d]+)\)')
        no_parenthesis_number = re.compile('^((?![()])\d)*$')
        has_numeric_inside = numeric_inside.search(field_value)
        has_non_numeric_inside = non_numeric_inside.search(field_value)
        is_just_number = no_parenthesis_number.search(field_value) is not None
        if has_numeric_inside:
            id_type = has_numeric_inside.group('outside')
            id_value = has_numeric_inside.group('inside')
            return Decimal(id_value), id_type
        if has_non_numeric_inside:
            id_value = has_non_numeric_inside.group('outside')
            id_type = has_non_numeric_inside.group('inside')
            try:
                return Decimal(id_value), id_type
            except InvalidOperation:
                return id_type, id_value
        if is_just_number:
            return Decimal(field_value), None
        return field_value, None
