import csv
import io
import os

import boto3


class Inquisitor:
    def __init__(self, table_name=None, graph_db_reader_address=None):
        if not table_name:
            table_name = os.getenv('TABLE_NAME', 'Leech')
        if not graph_db_reader_address:
            graph_db_reader_address = os.getenv('GRAPH_DB_READER_ENDPOINT')
        self._table_name = table_name
        self._graph_db_reader_address = graph_db_reader_address

    @classmethod
    def query_data(cls, query_source, query_args):
        query_fn = getattr(cls, query_source)
        return query_fn(**query_args)

    @classmethod
    def retrieve_csv_file(cls, id_source, file_name, key_name=None, **kwargs):
        header = None
        rows = {}
        bucket_name = kwargs.get('bucket_name')
        if not bucket_name:
            bucket_name = os.getenv('LEECH_BUCKET_NAME', 'algernonsolutions-leech')
        client = boto3.resource('s3')
        file_key = f'{id_source}/{file_name}'
        csv_file_request = client.Object(bucket_name, file_key).get()
        csv_file = csv_file_request['Body'].read().decode()
        with io.StringIO(csv_file, newline='\r\n') as csv_string:
            reader = csv.reader(csv_string, delimiter=',', quotechar='"')
            for row in reader:
                header_index = 0
                if header is None:
                    header = row
                    if key_name is None:
                        key_name = header[0]
                    continue
                row_dict = {}
                for entry in row:
                    try:
                        header_name = header[header_index]
                    except IndexError:
                        raise RuntimeError(
                            'the returned data from a csv query contained insufficient information to create the table')
                    row_dict[header_name] = entry
                    header_index += 1
                rows[row_dict[key_name]] = row_dict
        return rows

    @classmethod
    def retrieve_credible_client_download(cls, id_source, **kwargs):
        selected_fields = kwargs.get('selected_fields', {})
        return cls._retrieve_credible_download(id_source, 'Clients', selected_fields)

    @classmethod
    def retrieve_credible_employee_download(cls, id_source, **kwargs):
        selected_fields = kwargs.get('selected_fields', {})
        return cls._retrieve_credible_download(id_source, 'Employees', selected_fields)

    @classmethod
    def retrieve_credible_visit_download(cls, id_source, **kwargs):
        selected_fields = kwargs.get('selected_fields', {})
        start_date = kwargs.get('start_date', None)
        end_date = kwargs.get('end_date', None)
        return cls._retrieve_credible_download(id_source, 'ClientVisit', selected_fields, start_date, end_date)

    @classmethod
    def _retrieve_credible_download(cls, id_source, id_type, selected_fields, start_date=None, end_date=None):
        from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
        with CredibleFrontEndDriver(id_source) as driver:
            return driver.process_advanced_search(id_type, selected_fields, start_date, end_date)
