from datetime import datetime, timedelta

import pytest

from toll_booth.alg_obj.posts.inquisitor import Inquisitor


@pytest.mark.inquisitor
class TestInquisitor:
    @pytest.mark.client_downloader
    def test_credible_client_downloader(self, id_source):
        selected_fields = ['client_id', 'first_name', 'last_name']
        expected_fields = ['Client ID', 'Last Name', 'First Name']
        results = Inquisitor.retrieve_client_download(id_source, selected_fields)
        assert isinstance(results, list)
        for entry in results:
            assert isinstance(entry, dict)
            for expected in expected_fields:
                assert expected in entry

    @pytest.mark.emp_downloader
    def test_credible_emp_downloader(self, id_source):
        selected_fields = ['emp_id', 'first_name', 'last_name']
        expected_fields = ['Employee ID', 'Last Name', 'First Name']
        results = Inquisitor.retrieve_employee_download(id_source, selected_fields)
        assert isinstance(results, list)
        for entry in results:
            assert isinstance(entry, dict)
            for expected in expected_fields:
                assert expected in entry

    @pytest.mark.visit_downloader
    def test_credible_visit_downloader(self, id_source):
        selected_fields = ['emp_int_id', 'client_int_id', 'clientvisit_id']
        expected_fields = ['Service ID', 'Staff ID', 'Consumer ID']
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=2)
        results = Inquisitor.retrieve_visit_download(id_source, selected_fields, start_date, end_date)
        assert isinstance(results, list)
        for entry in results:
            assert isinstance(entry, dict)
            for expected in expected_fields:
                assert expected in entry

    @pytest.mark.csv_downloader
    def test_csv_downloader(self):
        id_source = 'PSI'
        file_name = 'caseloads.csv'
        results = Inquisitor.retrieve_csv_file(id_source, file_name, key_name='MedicaidNumber')
        print()
