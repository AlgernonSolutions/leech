import bs4
import dateutil
import pytest

from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver


@pytest.mark.post_process
class TestPostProcess:
    def test_post_process_visit_enrichment(self):
        encounter_id = '1147912'
        id_source = 'MBI'
        data_fields = []
        fields_of_interest = {
            'Service Type:': {
                'field_name': 'visit_type',
                'data_type': 'String'
            },
            'Program:': {
                'field_name': 'program',
                'data_type': 'String'
            },
            'Location:': {
                'field_name': 'location',
                'data_type': 'String'
            },
            'Recipient:': {
                'field_name': 'recipient',
                'data_type': 'String'
            },
            'Time In:': {
                'field_name': 'time_in',
                'data_type': 'Time'
            },
            'Time Out:': {
                'field_name': 'time_out',
                'data_type': 'Time'
            },
            'Date:': {
                'field_name': 'encounter_date',
                'data_type': 'Date'
            },
            'Duration:': {
                'field_name': 'duration',
                'data_type': 'Number'
            },
            'CPT Code:': {
                'field_name': 'cpt_code',
                'data_type': 'String'
            },
            'Non Billable:': {
                'field_name': 'non_billable',
                'data_type': 'Boolean'
            },
            'Transferred:': {
                'field_name': 'transfer_date',
                'data_type': 'DateTime'
            },
            'Approved': {
                'field_name': 'appr',
                'data_type': 'Boolean'
            }
        }
        header_data = {}
        encounter_data = []
        encounter_documentation = []
        with CredibleFrontEndDriver(id_source) as driver:
            results = driver.retrieve_client_encounter(encounter_id)
            encounter_soup = bs4.BeautifulSoup(results)
            tables = encounter_soup.find_all('table')
            encounter_header_table = tables[3]
            encounter_documentation_table = tables[7]
            encounter_data.extend(_filter_string(x) for x in encounter_header_table.strings if _filter_string(x))
            encounter_documentation.extend(_filter_string(x) for x in encounter_documentation_table.strings if _filter_string(x))
        for entry in encounter_data:
            if entry in fields_of_interest:
                entry_index = encounter_data.index(entry)
                entry_index += 1
                next_field = encounter_data[entry_index]
                field_name = fields_of_interest[entry]['field_name']
                data_type = fields_of_interest[entry]['data_type']
                header_data[field_name] = next_field
                data_fields.append({
                    'field_name': field_name,
                    'field_value': next_field,
                    'field_data_type': data_type,
                    'source_id_value': encounter_id,
                    'source_id_type': 'ClientVisit',
                    'source_id_source': id_source
                })
        datetime_in = dateutil.parser.parse(f"{header_data['encounter_date']} {header_data['time_in']}")
        data_fields.append({
            'field_name': 'datetime_in',
            'field_value': datetime_in.isoformat(),
            'field_data_type': 'DateTime',
            'source_id_value': encounter_id,
            'source_id_type': 'ClientVisit',
            'source_id_source': id_source
        })
        datetime_out = dateutil.parser.parse(f"{header_data['encounter_date']} {header_data['time_out']}")
        data_fields.append({
            'field_name': 'datetime_out',
            'field_value': datetime_out.isoformat(),
            'field_data_type': 'DateTime',
            'source_id_value': encounter_id,
            'source_id_type': 'ClientVisit',
            'source_id_source': id_source
        })
        return {'data_fields': data_fields}


def _filter_string(question_string):
    test_string = str(question_string)
    empty_characters = ['\n', '\r']
    for entry in empty_characters:
        test_string = test_string.replace(entry, '')
    test_string = test_string.strip()
    if test_string:
        return test_string
    return False
