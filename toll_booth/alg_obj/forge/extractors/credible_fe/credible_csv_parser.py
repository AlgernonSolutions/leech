import csv
import datetime
import io
from decimal import Decimal

import pytz


class CredibleCsvParser:
    _field_value_maps = {
        'Date': 'datetime',
        'Service Date': 'date',
        'Time In': 'datetime',
        'Time Out': 'datetime',
        'Service ID': 'number',
        'UTCDate': 'utc_datetime',
        'change_date': 'datetime',
        'by_emp_id': 'number',
        'Transfer Date': 'datetime',
        'Approved Date': 'datetime'
    }

    @classmethod
    def parse_csv_response(cls, csv_string, key_name=None):
        response = []
        if key_name:
            response = {}
        header = []
        first = True
        with io.StringIO(csv_string, newline='\r\n') as csv_string:
            reader = csv.reader(csv_string, delimiter=',', quotechar='"')
            for row in reader:
                header_index = 0
                row_entry = {}
                if first:
                    for entry in row:
                        header.append(entry)
                    first = False
                    continue
                for entry in row:
                    try:
                        header_name = header[header_index]
                    except IndexError:
                        raise RuntimeError(
                            'the returned data from a csv query contained insufficient information to create the table')
                    entry = cls._set_data_type(header_name, entry)
                    row_entry[header_name] = entry
                    header_index += 1
                if key_name:
                    key_value = row_entry[key_name]
                    response[key_value] = row_entry
                    continue
                response.append(row_entry)
        return response

    @classmethod
    def _set_data_type(cls, header_name, entry):
        data_type = cls._field_value_maps.get(header_name, 'string')
        if not entry:
            return None
        if data_type == 'string':
            entry = str(entry)
        if data_type == 'datetime':
            try:
                entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
            except ValueError:
                entry = f'{entry} 12:00:00 AM'
                entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
        if data_type == 'date':
            entry = datetime.datetime.strptime(entry, '%m/%d/%Y')
        if data_type == 'utc_datetime':
            try:
                entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
            except ValueError:
                entry = f'{entry} 12:00:00 AM'
                entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
            entry = entry.replace(tzinfo=pytz.UTC)
        if data_type == 'number':
            entry = Decimal(entry)
        return entry
