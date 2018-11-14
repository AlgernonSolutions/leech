import pytest


from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver, CredibleFrontEndExtractor
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry


@pytest.mark.credible_fe
@pytest.mark.usefixtures('mock_schema')
class TestCredibleFe:
    @pytest.mark.full_change_logs
    def test_get_full_change_logs(self, monitored_object_identifier_stem):
        identifier_stem = monitored_object_identifier_stem[0]
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        id_value = monitored_object_identifier_stem[1]
        object_type = identifier_stem.object_type
        schema_entry = SchemaVertexEntry.get(object_type)
        kwargs = {
            'identifier_stems': [{'identifier_stem': identifier_stem, 'id_value': id_value}],
            'id_source': identifier_stem.get('id_source')
        }
        extraction_profile = schema_entry.extract['CredibleFrontEndExtractor']
        kwargs.update(extraction_profile.extraction_properties)
        kwargs.update(identifier_stem.for_extractor)
        results = CredibleFrontEndExtractor.extract(**kwargs)
        print(results)

    def test_get_change_logs(self, monitored_object_identifier_stem):
        identifier_stem = monitored_object_identifier_stem[0]
        id_value = monitored_object_identifier_stem[1]
        extractor = CredibleFrontEndDriver(identifier_stem.get('id_source'))
        with extractor as extractor:
            field_values = extractor.get_change_logs(identifier_stem, id_value)
            print()

    def test_get_change_details(self, monitored_object_identifier_stem):
        identifier_stem = monitored_object_identifier_stem[0]
        id_value = monitored_object_identifier_stem[1]
        extractor = CredibleFrontEndDriver(identifier_stem.get('id_source'))
        with extractor as extractor:
            field_values = extractor.get_change_details(identifier_stem, id_value)
            print()

    def test_emp_id_search(self, employee_name):
        id_source = employee_name[2]
        last_name = employee_name[0]
        first_initial = employee_name[1]
        extractor = CredibleFrontEndDriver(id_source)
        with extractor as extractor:
            results = extractor.search_employees(last_name, first_initial)
            print()

    def test_get_data_dict(self):
        extractor = CredibleFrontEndDriver('MBI')
        with extractor as extractor:
            data_dict_data = extractor.get_data_dict_field_values('Clients', 8250, 14)
            print()
