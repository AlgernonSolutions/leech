import datetime
import io
from decimal import Decimal

import bs4
from requests import cookies
import requests
import csv
import re
from toll_booth.alg_obj.aws.squirrels.squirrel import Opossum
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


class CredibleFrontEndLoginException(Exception):
    pass


class CredibleFrontEndDriver:
    _base_stem = 'https://www.crediblebh.com'
    _field_value_urls = {
        'Clients': '/client/client_hipaalog.asp',
        'DataDict': '/common/hipaalog_datadict.asp',
        'ChangeDetail': '/common/hipaalog_details.asp',
        'EmployeeAdvanced': '/employee/list_emps_adv.asp'
    }
    _field_value_params = {
        'Clients': 'client_id'
    }
    _field_value_maps = {
        'Date': 'datetime',
        'Service ID': 'number',
        'UTCDate': 'utc_datetime',
        'change_date': 'datetime',
        'by_emp_id': 'number'
    }

    def __init__(self, id_source, credentials=None, session=None):
        if not session:
            session = requests.session()
        if not credentials:
            credentials = Opossum.get_untrustworthy_credentials(id_source)
        self._id_source = id_source
        self._session = session
        self._credentials = credentials

    def __enter__(self):
        session = requests.Session()
        cookie = self._get_cbh_cookie(session)
        if not cookie:
            raise CredibleFrontEndLoginException
        self._cookie = cookie
        self._session = session
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._logout(self._cookie, self._session)
        if exc_type:
            raise exc_val
        return True

    def get_clients_max_min(self):
        with self:
            data = {
                "allergyexists": "",
                "anydiagnosis": 1,
                "axis1_id": "",
                "axis2_id": "",
                "btn_filter": "Filter",
                "btn_loadsaved": "",
                "btn_where": "",
                "client_id": 1,
                "client_nameid": "Consumer Name/ID",
                "client_status_f": "ALL ACTIVE",
                "emp_status": 1,
                "first_name": 1,
                "hiFAxisDesc": "",
                "hiFAxisICD09": "",
                "hiFAxisICD10": "",
                "hiUlSelectedAxisHtml": "",
                "ins_id_f": "Insurance ID",
                "labexists": "",
                "last_name": 1,
                "medexists": "",
                "non_billable": "",
                "num8": 1,
                "payer_id": "",
                "payertype_id": "",
                "period_end": "Period End",
                "period_start": "Period Start",
                "phonetic": "Phonetic Name",
                "primary_emp_id": "",
                "program_id": "",
                "save_name": "Save Name",
                "search_team": "",
                "searchclientssaves_id": "",
                "sort_order": 1,
                "sortby": "client_id",
                "sortby2": "",
                "ssn_f": "SSN",
                "submitform": "true",
                "team_id": "",
                "text4": 1,
                "visittype_id": "",
                "wh_andor": "AND",
                "wh_cmp1": "=",
                "wh_cmp2": "=",
                "wh_fld1": "",
                "wh_fld2": "",
                "wh_val1": "Value",
                "wh_val2": "Value"
            }
            url = "https://www.crediblebh.com/client/list_clients_adv.asp"
            response = self._session.post(url=url, data=data)
            credible_html = response.content
            regex = r"(<a href='/client/my_cw_clients.asp\?client_id=)(\d+)"
            matches = re.finditer(regex, credible_html, re.MULTILINE)
            all_ids = []
            for match in matches:
                all_ids.append(int(match[2]))
            max_id = max(all_ids)
            min_id = min(all_ids)
        return max_id, min_id

    def search_employees(self, last_name, first_initial):
        data = {
            'submitform': 'true',
            'btn_export': ' Export ',
            'wh_fld1': 'last_name',
            'wh_cmp1': 'LIKE',
            'wh_val1': f'*{last_name}*',
            'wh_andor': 'AND',
            'wh_fld2': 'first_name',
            'wh_cmp2': 'LIKE',
            'wh_val2': f'{first_initial}*',
            'emp_id': 1,
            'emp_status': 1
        }
        url = self._base_stem + self._field_value_urls['EmployeeAdvanced']
        response = self._session.post(url, data=data)
        possible_employees = self._parse_csv_response(response.text)
        if len(possible_employees) == 1:
            for entry in possible_employees:
                for cell_name, row in entry.items():
                    if 'ID' in cell_name:
                        return Decimal(row)
        if not possible_employees:
            raise RuntimeError(
                'could not find employee with last_name: %s, first_initial: %s' % (last_name, first_initial))

    def get_monitor_extraction(self, object_type):
        return

    def get_data_dict_field_values(self, id_type, id_value, data_dict_id):
        values = []
        pattern = '(<table id=\"datadicttable\" border=\"0\" cellpadding=\"0\" cellspacing=\"0\" width=\"100%\">)(?P<contents>[\s\S]+)(<\/table>)'
        emp_id_pattern = '\((?P<emp_id>[\d]+)\)'
        param_name = self._field_value_params[id_type]
        params = {
            'data_dict_id': data_dict_id,
            param_name: id_value
        }
        url = self._base_stem + self._field_value_urls['DataDict']
        response = self._session.get(url, params=params)
        compiled_match = re.compile(pattern)
        compiled_emp_id = re.compile(emp_id_pattern)
        matches = compiled_match.search(response.text)
        table_contents = matches.group('contents')
        table_soup = bs4.BeautifulSoup(table_contents)
        rows = table_soup.find_all('tr')
        for row in rows:
            entry = {}
            fields = row.find_all('td')
            pointer = 0
            field_names = ['change_date', 'by_emp_id', 'old_value', 'new_value']
            for field in fields:
                field_name = field_names[pointer]
                field_value = field.string
                if field_name == 'by_emp_id':
                    emp_id_match = compiled_emp_id.search(field_value)
                    field_value = emp_id_match.group('emp_id')
                field_value = self._set_data_type(field_name, field_value)
                entry[field_name] = field_value
                pointer += 1
            if entry:
                values.append(entry)
        return values

    def get_change_logs(self, identifier_stem, id_value):
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        id_type = identifier_stem.get('id_type')
        param_name = self._field_value_params[id_type]
        param_value = identifier_stem.get('id_value')
        url = self._base_stem + self._field_value_urls[id_type]
        data = {
            param_name: param_value,
            'start_date': self._format_datetime_id_value(id_value),
            'changelogcategory_id': '',
            'changelogtype_id': '',
            'btn_export': 'Export'
        }
        response = self._session.post(url, data=data)
        csv_response = self._parse_csv_response(response.text, key_name='UTCDate')
        return csv_response

    def get_emp_ids(self, identifier_stem, id_value):
        pass

    def _get_emp_ids(self):
        pass

    def get_change_details(self, identifier_stem, id_value):
        details = {}
        change_details, page_number = self._get_change_details(identifier_stem, id_value)
        details.update(change_details)
        while page_number is not None:
            new_details, page_number = self._get_change_details(identifier_stem, id_value, page_number)
            details.update(new_details)
        return details

    def _get_change_details(self, identifier_stem, id_value, page_number=1):
        change_details = {}
        row_pattern = '(<table border=\"\d\"\scellpadding=\"\d\"\scellspacing=\"\d\"\swidth=\"[\d]+%\">)(?P<rows>[.\s\S]+?)(<\/table>)'
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        id_type = identifier_stem.get('id_type')
        param_name = self._field_value_params[id_type]
        param_value = identifier_stem.get('id_value')
        url = self._base_stem + self._field_value_urls[id_type]
        data = {
            param_name: param_value,
            'start_date': self._format_datetime_id_value(id_value),
            'changelogcategory_id': '',
            'changelogtype_id': '',
            'page': page_number
        }
        page_number += 1
        response = self._session.post(url, data=data)
        row_match = re.compile(row_pattern).search(response.text).group('rows')
        row_soup = bs4.BeautifulSoup(row_match)
        table_rows = row_soup.find_all('a')
        if len(table_rows) <= 6:
            return [], None
        for row in table_rows:
            if 'href' in row.attrs and 'title' in row.attrs and row.attrs['href'] != '#':
                target = row.attrs['href']
                changelog_id = re.compile('changelog_id=(?P<changelog_id>\d+)').search(target).group('changelog_id')
                changes = self.__get_change_details(changelog_id)
                containing_row = row.parent.parent
                change_date_string = containing_row.contents[15].string
                change_date = datetime.datetime.strptime(change_date_string, '%m/%d/%Y %I:%M:%S %p')
                change_date = change_date.timestamp()
                change_date = datetime.datetime.fromtimestamp(change_date, tz=datetime.timezone.utc)
                change_details[change_date] = changes
        return change_details, page_number

    def __get_change_details(self, changelog_id):
        changes = []
        url = self._base_stem + self._field_value_urls['ChangeDetail']
        response = self._session.get(url, params={'changelog_id': changelog_id})
        detail_soup = bs4.BeautifulSoup(response.content)
        all_rows = detail_soup.find_all('tr')
        interesting_rows = all_rows[4:]
        for row in interesting_rows:
            changes.append({
                'id_name': str(row.contents[1].string).lower(),
                'old_value': str(row.contents[3].string),
                'new_value': str(row.contents[5].string)
            })
        return changes

    def _get_cbh_cookie(self, session):
        attempts = 0
        while attempts < 3:
            try:
                jar = cookies.RequestsCookieJar()
                api_url = "https://login-api.crediblebh.com/api/Authenticate/CheckLogin"
                index_url = "https://ww7.crediblebh.com/index.aspx"
                first_payload = {'UserName': self._credentials['username'],
                                 'Password': self._credentials['password'],
                                 'DomainName': self._credentials['domain_name']}
                headers = {'DomainName': self._credentials['domain_name']}
                post = session.post(api_url, json=first_payload, headers=headers)
                response_json = post.json()
                session_cookie = response_json['SessionCookie']
                jar.set('SessionId', session_cookie, domain='.crediblebh.com', path='/')
                second_payload = {'SessionId': session_cookie}
                second_post = session.post(index_url, data=second_payload, cookies=jar)
                history = second_post.history
                cbh_response = history[0]
                cbh_cookies = cbh_response.cookies
                session.cookies = cbh_cookies
                return cbh_cookies
            except KeyError or ConnectionError:
                attempts += 1
        return 0

    @classmethod
    def _format_datetime_id_value(cls, id_value):
        credible_format = '%m/%d/%Y'
        return id_value.strftime(credible_format)

    @classmethod
    def _parse_csv_response(cls, csv_string, key_name=None):
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
            entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
        if data_type == 'utc_datetime':
            entry = datetime.datetime.strptime(entry, '%m/%d/%Y %I:%M:%S %p')
            entry = entry.timestamp()
            entry = datetime.datetime.fromtimestamp(entry, tz=datetime.timezone.utc)
        if data_type == 'number':
            entry = Decimal(entry)
        return entry

    @classmethod
    def _logout(cls, cookie_jar, session):
        logout_url = 'https://ww7.crediblebh.com/secure/logout.aspx'
        session.get(
            logout_url,
            cookies=cookie_jar
        )
