import collections
from xml.dom import minidom

import requests
import dateutil.parser

from toll_booth.alg_obj.aws.squirrels.squirrel import Opossum


class CredibleDriver:
    def __init__(self, domain_name, api_key=None, session=None):
        if not api_key:
            api_key = Opossum.get_untrustworthy_export_key(domain_name)
        if not session:
            session = requests.Session()
        self._domain_name = domain_name
        self._api_key = api_key
        self._session = session

    @property
    def api_key(self):
        return self._api_key

    @property
    def domain_name(self):
        return self._domain_name

    def run(self, sql):
        return CredibleReport.from_sql(self._domain_name, sql, self._api_key, self._session).flattened

    def get_remote_max_min(self, id_type, id_name):
        return RemoteMaxMin(self._domain_name, id_type, id_name, self._api_key, self._session)


class CredibleReport:
    def __init__(self, data):
        self.data = data

    def __getitem__(self, item):
        return self.data[item]

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def __bool__(self):
        if self.data:
            return True
        return False

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def items(self):
        return self.data.items()

    @classmethod
    def from_sql(cls, domain_name, sql, access_key=None, session=None):
        data = get_report_as_dict(domain_name, sql, access_key, session)
        return cls(data)

    @property
    def flattened(self):
        flat = []
        for value in self.values():
            if isinstance(value, list):
                flat.extend(value)
            else:
                flat.append(value)
        return flat


class RemoteMaxMin:
    def __init__(self, id_source, id_type, id_name, api_key, session):
        # noinspection SqlNoDataSourceInspection
        query = f'SELECT MIN({id_name}) as min, MAX({id_name}) as max FROM {id_type}'
        report = CredibleReport.from_sql(id_source, query, api_key, session)
        for entry in report.flattened:
            self._min = entry['min']
            self._max = entry['max']

    @property
    def max_id(self):
        return int(self._max)

    @property
    def min_id(self):
        return int(self._min)

    @property
    def range(self):
        return self.max_id - self.min_id


def get_google_formatted_report(domain_name, sql, access_key=None, session=None):
    try:
        document = get_report(
            domain_name=domain_name,
            sql=sql,
            access_key=access_key,
            session=session
        )
        if document is 0:
            return []
        else:
            tables = parse_tables(document)
            return tables
    except AttributeError as err:
        print(err)
        print('report with sql: %s for domain_name %s returned no values' % (sql, domain_name))
        return []


def get_report_as_dict(domain_name, sql, access_key=None, session=None):
    dict_report = collections.OrderedDict()
    report = get_google_formatted_report(
        domain_name=domain_name,
        sql=sql,
        access_key=access_key,
        session=session
    )
    try:
        header = report.pop(0)
    except IndexError:
        return dict_report
    for row in report:
        pk_guess = row[0]
        count = 0
        report_line = collections.OrderedDict()
        for field in row:
            report_line[header[count]] = field
            count += 1
        if pk_guess in dict_report:
            current_value = dict_report[pk_guess]
            if isinstance(current_value, list):
                dict_report[pk_guess].append(report_line)
            else:
                dict_report[pk_guess] = [current_value, report_line]
        else:
            dict_report[pk_guess] = report_line
    return dict_report


def parse_header(document):
    header = []
    pointer = 1
    header_row_element = document.getElementsByTagName(
        'xs:sequence'
    ).item(pointer)
    header_rows = header_row_element.getElementsByTagName(
        'xs:element'
    )
    for header_row in header_rows:
        header_entry = {
            'name': header_row.getAttribute('name'),
            'type': header_row.getAttribute('type').replace('xs:', '')
        }
        header.append(header_entry)
    return header


def parse_tables(document):
    target = 'Table1'
    header = []
    header_data = parse_header(document)
    data_sets = document.getElementsByTagName('NewDataSet')
    for header_row in header_data:
        header.append(header_row['name'])
    table = [header]
    if len(data_sets) is 0:
        return []
    else:
        data_sets = data_sets[0]
        for data_set in data_sets.childNodes:
            if data_set.nodeType == data_set.ELEMENT_NODE:
                if data_set.localName == target:
                    row = []
                    row_dict = {}
                    for entry in data_set.childNodes:
                        if entry.nodeType == entry.ELEMENT_NODE:
                            if entry.firstChild:
                                data = entry.firstChild.data
                            else:
                                data = ''
                            header_field = entry.nodeName
                            if len(data) > 45000:
                                data = data[:45000]
                            row_dict[header_field] = data
                    for header_entry in header_data:
                        try:
                            data = row_dict[header_entry['name']]
                            data_type = header_entry['type']
                            if data_type in ('int', 'short', 'double'):
                                try:
                                    data = int(data)
                                except ValueError:
                                    data = float(data)
                            elif data_type == 'string':
                                data = data.replace('<b>', '')
                                data = data.replace('</b>', '')
                            elif data_type == 'dateTime':
                                data = dateutil.parser.parse(data)
                            elif data_type == 'boolean':
                                data = data == 'true'
                            else:
                                data = data
                        except KeyError:
                            data = ''
                        row.append(data)
                    table.append(row)
        return table


def get_report(domain_name, sql, access_key=None, session=None):
    completed = False
    tries = 0
    url = 'https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet'

    if not access_key:
        access_key = Opossum.get_untrustworthy_export_key(domain_name)
    payload = {
        'connection': access_key,
        'start_date': '',
        'end_date': '',
        'custom_param1': sql,
        'custom_param2': '',
        'custom_param3': ''
    }
    while not completed and tries < 3:
        try:
            if session:
                cr = session.post(url, data=payload)
            else:
                cr = requests.post(url, parms=payload)
            raw_xml = cr.content
            document = minidom.parseString(raw_xml).childNodes[0]
            if len(document.childNodes) > 0:
                return document
            else:
                tries += 1
        except requests.exceptions.ConnectionError:
            tries += 1
    print('report with sql: %s for domain_name %s could not be fetched' % (sql, domain_name))
    return []
