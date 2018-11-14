import pytest


from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver, CredibleFrontEndExtractor


@pytest.mark.credible_fe
class TestCredibleFe:
    @pytest.mark.full_change_logs
    def test_get_full_change_logs(self, monitored_object_identifier_stem):
        identifier_stem = monitored_object_identifier_stem[0]
        id_value = monitored_object_identifier_stem[1]
        kwargs = {
            'identifier_stems': [{'identifier_stem': identifier_stem, 'id_value': id_value}],
            'id_source': identifier_stem.get('id_source'),
            'mapping': {
              "DCDBH": {
                "Clients": {
                  "Date": {
                    "name": "change_date",
                    "mutation": None
                  },
                  "UTCDate": {
                    "name": "change_date_utc",
                    "mutation": None
                  },
                  "Description": {
                    "name": "change_description",
                    "mutation": None
                  },
                  "Action": {
                    "name": "action",
                    "mutation": None
                  },
                  "Service ID": {
                    "name": "clientvisit_id",
                    "mutation": None
                  },
                  "Record": {
                    "name": "record",
                    "mutation": "split_record_id"
                  },
                  "Consumer Name": {
                    "name": "client_id",
                    "mutation": "split_client_name"
                  }
                }
              },
              "MBI": {
                "Clients": {
                  "Date": {
                    "name": "change_date",
                    "mutation": None
                  },
                  "UTCDate": {
                    "name": "change_date_utc",
                    "mutation": None
                  },
                  "Description": {
                    "name": "change_description",
                    "mutation": None
                  },
                  "Action": {
                    "name": "action",
                    "mutation": None
                  },
                  "Service ID": {
                    "name": "clientvisit_id",
                    "mutation": None
                  },
                  "Record": {
                    "name": "record",
                    "mutation": "split_record_id"
                  },
                  "Consumer Name": {
                    "name": "client_id",
                    "mutation": "split_client_name"
                  }
                }
              },
              "default": {
                "Clients": {
                  "Date": {
                    "name": "change_date",
                    "mutation": None
                  },
                  "UTCDate": {
                    "name": "change_date_utc",
                    "mutation": None
                  },
                  "Description": {
                    "name": "change_description",
                    "mutation": None
                  },
                  "Action": {
                    "name": "action",
                    "mutation": None
                  },
                  "Service ID": {
                    "name": "clientvisit_id",
                    "mutation": None
                  },
                  "Record": {
                    "name": "record",
                    "mutation": "split_record_id"
                  },
                  "Client Name": {
                    "name": "client_id",
                    "mutation": "split_client_name"
                  }
                }
              }
            }
        }
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
