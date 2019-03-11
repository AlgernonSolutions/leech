import datetime
import logging
import re
from decimal import Decimal

import bs4
import pytz
import requests
from requests import cookies
from retrying import retry

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.aws.squirrels.squirrel import Opossum
from toll_booth.alg_obj.forge.extractors.credible_fe.credible_csv_parser import CredibleCsvParser
from toll_booth.alg_obj.forge.extractors.credible_fe.cache import CachedEmployeeIds

_base_stem = 'https://www.crediblebh.com'
_url_stems = {
    'Clients': '/client/client_hipaalog.asp',
    'Employees': '/employee/emp_hipaalog.asp',
    'DataDict': '/common/hipaalog_datadict.asp',
    'ChangeDetail': '/common/hipaalog_details.asp',
    'Employee Advanced': '/employee/list_emps_adv.asp',
    'Global': '/admin/global_hipaalog.aspx',
    'Encounter': '/visit/clientvisit_view.asp'
}


def _login_required(function):
    def wrapper(*args, **kwargs):
        driver = args[0]
        driver.credentials.refresh_if_stale(session=driver.session)
        return function(*args, **kwargs)

    return wrapper


class CredibleFrontEndLoginException(Exception):
    pass


class CredibleLoginCredentials(AlgObject):
    def __init__(self, domain_name, cookie_value, time_generated):
        self._domain_name = domain_name
        self._cookie_value = cookie_value
        self._time_generated = time_generated

    @property
    def domain_name(self):
        return self._domain_name

    @property
    def cookie_value(self):
        return self._cookie_value

    @property
    def time_generated(self):
        return self._time_generated

    @property
    def as_request_cookie_jar(self):
        cookie_jar = requests.cookies.RequestsCookieJar()
        cookie_args = {
            'name': 'cbh',
            'value': self._cookie_value,
            'domain': '.crediblebh.com'
        }
        credible_cookie = requests.cookies.create_cookie(**cookie_args)
        cookie_jar.set_cookie(credible_cookie)
        return cookie_jar

    @classmethod
    def retrieve(cls, id_source, session=None, username=None, password=None, domain_name=None):
        time_generated = datetime.datetime.now()
        if not session:
            session = requests.Session()
        if not username or not password:
            credentials = Opossum.get_untrustworthy_credentials(id_source)
            username = credentials['username']
            password = credentials['password']
            domain_name = credentials['domain_name']
        attempts = 0
        while attempts < 3:
            try:
                jar = cookies.RequestsCookieJar()
                api_url = "https://login-api.crediblebh.com/api/Authenticate/CheckLogin"
                index_url = "https://ww7.crediblebh.com/index.aspx"
                first_payload = {'UserName': username,
                                 'Password': password,
                                 'DomainName': domain_name}
                headers = {'DomainName': domain_name}
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
                cookie_values = getattr(cbh_cookies, '_cookies')
                credible_value = cookie_values['.crediblebh.com']['/']['cbh'].value
                return cls(domain_name, credible_value, time_generated)
            except KeyError or ConnectionError:
                attempts += 1
        raise CredibleFrontEndLoginException()

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['domain_name'], json_dict['cookie_value'], json_dict['time_generated'])

    def is_stale(self, lifetime_minutes=30):
        cookie_age = (datetime.datetime.now() - self._time_generated).seconds
        return cookie_age >= (lifetime_minutes * 60)

    def refresh_if_stale(self, lifetime_minutes=30, **kwargs):
        if self.is_stale(lifetime_minutes):
            self.refresh(**kwargs)

    def refresh(self, session=None, username=None, password=None):
        new_credentials = self.retrieve(self._domain_name, session, username, password)
        self._cookie_value = new_credentials.cookie_value

    def destroy(self, session=None):
        if not session:
            session = requests.Session()
        logout_url = 'https://ww7.crediblebh.com/secure/logout.aspx'
        session.get(
            logout_url,
            cookies=self.as_request_cookie_jar
        )


class CredibleFrontEndDriver:
    _monitor_extract_stems = {
        'Employees': '/employee/list_emps_adv.asp',
        'Clients': '/client/list_clients_adv.asp',
        'ClientVisit': '/visit/list_visits_adv.asp'
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

    def __init__(self, id_source, session=None, credentials=None):
        if not session:
            session = requests.Session()
        if not credentials:
            credentials = CredibleLoginCredentials.retrieve(id_source, session=session)
        self._id_source = id_source
        session.cookies = credentials.as_request_cookie_jar
        self._session = session
        self._credentials = credentials

    def __enter__(self):
        session = requests.Session()
        if not self._credentials:
            credentials = CredibleLoginCredentials.retrieve(self._id_source, session=session)
            self._credentials = credentials
        session.cookies = self._credentials.as_request_cookie_jar
        self._session = session
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._credentials.destroy(self._session)
        if exc_type:
            raise exc_val
        return True

    @property
    def credentials(self):
        return self._credentials

    @property
    def session(self):
        return self._session

    @_login_required
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
            credible_html = response.text
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
        url = _base_stem + _url_stems['Employee Advanced']
        response = self._session.post(url, data=data)
        possible_employees = CredibleCsvParser.parse_csv_response(response.text)
        if len(possible_employees) == 1:
            for entry in possible_employees:
                for cell_name, row in entry.items():
                    if 'ID' in cell_name:
                        return Decimal(row)
        if not possible_employees:
            raise RuntimeError(
                'could not find employee with last_name: %s, first_initial: %s' % (last_name, first_initial))

    # @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
    @_login_required
    def get_monitor_extraction(self, **kwargs):
        mapping = kwargs['mapping']
        id_type = kwargs['id_type']
        source_mapping = mapping.get(self._id_source, mapping['default'])
        object_field_names = source_mapping['ExternalId'][id_type]
        data = {
            'submitform': 'true',
            'btn_export': ' Export ',
            object_field_names['alg_name']: 1
        }
        url = _base_stem + self._monitor_extract_stems[id_type]
        response = self._session.post(url, data=data)
        possible_objects = CredibleCsvParser.parse_csv_response(response.text)
        return [x[object_field_names['internal_name']] for x in possible_objects]

    def get_data_dict_field_values(self, id_type, id_value, data_dict_id):
        values = []
        pattern = '(<table id=\"datadicttable\" border=\"0\" cellpadding=\"0\" cellspacing=\"0\" width=\"100%\">)(?P<contents>[\s\S]+)(<\/table>)'
        emp_id_pattern = '\((?P<emp_id>[\d]+)\)'
        param_name = self._field_value_params[id_type]
        params = {
            'data_dict_id': data_dict_id,
            param_name: id_value
        }
        url = _base_stem + _url_stems['DataDict']
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

    @_login_required
    def get_global_hipaa_log(self, id_type, category_id, start_date, end_date):
        id_types = {
            'Employees': 'employee',
            'Clients': 'client'
        }
        url = _base_stem + _url_stems['Global']
        data = {
            'run_report': True,
            'rpt': 'RptGlbHipaaLog',
            'btn_export': 'Export',
            'rname': 'Global HIPAA Log',
            'prompt_type1': 'DD',
            'prompt_lbl1': 'Entity',
            'prompt1': id_types[id_type],
            'prompt_type2': 'DD',
            'prompt_lbl2': 'Category',
            'prompt2': category_id,
            'prompt_type3': 'DD',
            'prompt_lbl3': 'Action',
            'prompt3': '',
            'prompt_type4': 'DATE',
            'prompt_lbl4': 'Start Date',
            'prompt4': self._format_datetime_value(start_date),
            'prompt_type5': 'DATE',
            'prompt_lbl5': 'End Date',
            'prompt5': self._format_datetime_value(end_date)
        }
        response = self._session.post(url, data=data)
        if response.status_code != 200:
            raise RuntimeError('could not get the change logs for %s' % data)
        csv_response = CredibleCsvParser.parse_csv_response(response.text, key_name='UTCDate')
        return csv_response

    @_login_required
    def get_change_logs(self, **kwargs):
        url = _base_stem + _url_stems[kwargs['driving_id_type']]
        data = {
            kwargs['driving_id_name']: kwargs['driving_id_value'],
            'start_date': self._format_datetime_id_value(kwargs['local_max_value']),
            'changelogcategory_id': kwargs.get('category_id', ''),
            'changelogtype_id': '',
            'btn_export': 'Export'
        }
        response = self._session.post(url, data=data)
        if response.status_code != 200:
            raise RuntimeError(f'could not get the change logs for {data}, response code: {response.status_code}')
        csv_response = CredibleCsvParser.parse_csv_response(response.text)
        return csv_response

    def enrich_change_logs(self, **kwargs):
        enrichment = {}
        cached_emp_ids = CachedEmployeeIds()
        kwargs['cached_emp_ids'] = cached_emp_ids
        enriched_data, page_number = self._get_changelog_page(**kwargs)
        for enriched_name, entry in enriched_data.items():
            enrichment[enriched_name] = entry
        while page_number is not None:
            kwargs['page_number'] = page_number
            enriched_data, page_number = self._get_changelog_page(**kwargs)
            for enriched_name, entry in enriched_data.items():
                enrichment[enriched_name].update(entry)
        return enrichment

    def _get_changelog_page(self, **kwargs):
        changelog_data = {}
        row_pattern = re.compile(
            '(<table border=\"\d\"\scellpadding=\"\d\"\scellspacing=\"\d\"\swidth=\"[\d]+%\">)(?P<rows>[.\s\S]+?)(</table>)')
        url = _base_stem + _url_stems[kwargs['driving_id_type']]
        page_number = kwargs.get('page_number', 1)
        data = {
            kwargs['driving_id_name']: kwargs['driving_id_value'],
            'start_date': self._format_datetime_id_value(kwargs['local_max_value']),
            'changelogcategory_id': kwargs.get('category_id', ''),
            'changelogtype_id': kwargs.get('action_id', ''),
            'page': page_number
        }
        page_number += 1
        response = self._session.post(url, data=data)
        if response.status_code != 200:
            raise RuntimeError('could not retrieve change details for %s' % data)
        row_match = re.compile(row_pattern).search(response.text).group('rows')
        row_soup = bs4.BeautifulSoup(row_match, features='html.parser')
        table_rows = row_soup.find_all('tr')
        if len(table_rows) <= 3:
            page_number = None
        if kwargs.get('get_emp_ids'):
            emp_ids = self._strain_emp_ids(table_rows, kwargs.get('cached_emp_ids'))
            changelog_data['emp_ids'] = emp_ids
        if kwargs.get('get_details'):
            change_details = self._strain_change_details(row_soup)
            changelog_data['change_details'] = change_details
        return changelog_data, page_number

    @_login_required
    def get_caseload_report(self):
        data = {
            'btn_filter': 'Export File',
            'rpt': 'RptCaseLoadNoRoles',
            'prompt1': '',
            'prompt2': '',
            'prompt3': 1,
            'prompt4': '',
            'prompt5': '',
            'blnroles': 0,
            'blnseverity': 0
        }
        url = 'https://reports.crediblebh.com/reports/caseload.asp'
        response = self._session.post(url, data=data)

    @_login_required
    def process_advanced_search(self, id_type, selected_fields, start_date=None, end_date=None):
        credible_date_format = '%m/%d/%Y'
        url = _base_stem + self._monitor_extract_stems[id_type]
        data = {
            'submitform': 'true',
            'btn_export': ' Export ',
        }
        data.update(selected_fields)
        if start_date:
            data['start_date'] = start_date.strftime(credible_date_format)
        if end_date:
            data['end_date'] = end_date.strftime(credible_date_format)
        response = self._session.post(url, data=data)
        possible_objects = CredibleCsvParser.parse_csv_response(response.text)
        return possible_objects

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
    @_login_required
    def retrieve_changelog_page(self, url, **kwargs):
        page_number = kwargs.get('page_number', 1)
        data = {
            kwargs['driving_id_name']: kwargs['driving_id_value'],
            'start_date': self._format_datetime_id_value(kwargs['local_max_value']),
            'changelogcategory_id': kwargs.get('category_id', ''),
            'changelogtype_id': kwargs.get('action_id', ''),
            'page': page_number
        }
        response = self._session.post(url, data=data)
        if response.status_code != 200:
            raise RuntimeError('could not retrieve changelogs for %s' % data)
        return response.text

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
    @_login_required
    def retrieve_change_detail_page(self, url, changelog_id):
        response = self._session.get(url, params={'changelog_id': changelog_id})
        if response.status_code != 200:
            raise RuntimeError('could not retrieve change details for %s' % changelog_id)
        return response.text

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
    @_login_required
    def retrieve_emp_id_search(self, url, last_name, first_initial):
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
        response = self._session.post(url, data=data)
        return response.text

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
    @_login_required
    def retrieve_client_id_search(self, url, last_name, first_initial):
        data = {
            'submitform': 'true',
            'btn_export': ' Export ',
            'wh_fld1': 'c.last_name',
            'wh_cmp1': 'LIKE',
            'wh_val1': f'*{last_name}*',
            'wh_andor': 'AND',
            'wh_fld2': 'c.first_name',
            'wh_cmp2': 'LIKE',
            'wh_val2': f'{first_initial}*',
            'client_id': 1
        }
        response = self._session.post(url, data=data)
        return response.text

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
    @_login_required
    def retrieve_client_encounter(self, encounter_id):
        url = _base_stem + _url_stems['Encounter']
        response = self._session.get(url, data={'clientvisit_id': encounter_id})
        if response.status_code != 200:
            raise RuntimeError(f'could not get the encounter data for {encounter_id}, response code: {response.status_code}')
        return response.text

    @_login_required
    def set_client_case_manager(self, **kwargs):
        url_stem = "/client/client_employee.asp"
        client_id = kwargs['client_id']
        case_manager_emp_id = kwargs['case_manager_emp_id']
        data = {
            'client_id': client_id,
            'emp_id': case_manager_emp_id,
            'case_manager': 1,
            'cm_action': 'A'
        }
        url = _base_stem + url_stem
        results = self._session.get(url, data=data)

    def _strain_emp_ids(self, table_rows, cached_emp_ids):
        name_pattern = re.compile("(?P<last_name>\w+),\s+(?P<first_initial>\w)")
        emp_ids = {}
        for row in table_rows:
            if 'style' in row.attrs:
                utc_change_date_string = row.contents[15].string
                try:
                    change_date_utc = datetime.datetime.strptime(utc_change_date_string, '%m/%d/%Y %I:%M:%S %p')
                except ValueError:
                    utc_change_date_string = f'{utc_change_date_string} 12:00:00 AM'
                    change_date_utc = datetime.datetime.strptime(utc_change_date_string, '%m/%d/%Y %I:%M:%S %p')
                change_date_utc = change_date_utc.replace(tzinfo=pytz.UTC)
                done_by_entry = row.contents[9]
                done_by_name = done_by_entry.string
                matches = name_pattern.search(done_by_name)
                last_name = matches.group('last_name')
                first_initial = matches.group('first_initial')
                emp_id = cached_emp_ids.get_emp_id(last_name, first_initial)
                if emp_id is None:
                    try:
                        emp_id = self.search_employees(last_name, first_initial)
                    except RuntimeError:
                        logging.warning('could not determine the emp_id from their name: %s' %
                                        f'{last_name}, {first_initial}, using default value of 0')
                        emp_id = 0
                    cached_emp_ids.add_emp_id(last_name, first_initial, emp_id)
                emp_ids[change_date_utc.timestamp()] = emp_id
        return emp_ids

    def _strain_change_details(self, row_soup):
        change_details = {}
        table_rows = row_soup.find_all('a')
        if len(table_rows) <= 6:
            return {}
        for row in table_rows:
            if 'clid' in row.attrs:
                changelog_id = row.attrs['clid']
                # changelog_id = re.compile('changelog_id=(?P<changelog_id>\d+)').search(target).group('changelog_id')
                changes = self.__get_change_details(changelog_id)
                containing_row = row.parent.parent
                utc_change_date_string = containing_row.contents[15].string
                try:
                    change_date_utc = datetime.datetime.strptime(utc_change_date_string, '%m/%d/%Y %I:%M:%S %p')
                except ValueError:
                    utc_change_date_string = f'{utc_change_date_string} 12:00:00 AM'
                    change_date_utc = datetime.datetime.strptime(utc_change_date_string, '%m/%d/%Y %I:%M:%S %p')
                change_date_utc = change_date_utc.replace(tzinfo=pytz.UTC)
                change_details[change_date_utc.timestamp()] = changes
        return change_details

    def get_emp_ids(self, **kwargs):
        emp_ids = {}
        emps, page_number = self._get_emp_ids(**kwargs)
        emp_ids.update(emps)
        while page_number is not None:
            kwargs['page_number'] = page_number
            emps, page_number = self._get_emp_ids(**kwargs)
            emp_ids.update(emps)
        return emp_ids

    def _get_emp_ids(self, **kwargs):
        emp_ids = {}
        row_pattern = re.compile(
            '(<table border=\"\d\"\scellpadding=\"\d\"\scellspacing=\"\d\"\swidth=\"[\d]+%\">)(?P<rows>[.\s\S]+?)(</table>)')
        page_number = kwargs.get('page_number', 1)
        url = _base_stem + _url_stems[kwargs['driving_id_type']]
        data = {
            kwargs['driving_id_name']: kwargs['driving_id_value'],
            'start_date': self._format_datetime_id_value(kwargs['local_change_log_id_value']),
            'changelogcategory_id': '',
            'changelogtype_id': '',
            'page': page_number
        }
        page_number += 1
        response = self._session.post(url, data=data)
        row_match = row_pattern.search(response.text).group('rows')
        row_soup = bs4.BeautifulSoup(row_match)
        table_rows = row_soup.find_all('tr')
        if len(table_rows) <= 3:
            return [], None
        for row in table_rows:
            if 'style' in row.attrs:
                utc_change_date = row.contents[15].string
                entry = datetime.datetime.strptime(utc_change_date, '%m/%d/%Y %I:%M:%S %p')
                entry = entry.timestamp()
                change_time = datetime.datetime.fromtimestamp(entry, tz=datetime.timezone.utc)
                emp_entry = row.contents[3]
                numeric_inside = re.compile('(?P<outside>[\w\s]+?)\s*\((?P<inside>[\d]+)\)')
                match = numeric_inside.search(emp_entry.string).group('inside')
                emp_id = Decimal(match)
                emp_ids[change_time] = emp_id
        return emp_ids, page_number

    def get_change_details(self, **kwargs):
        details = {}
        change_details, page_number = self._get_change_details(**kwargs)
        details.update(change_details)
        while page_number is not None:
            kwargs['page_number'] = page_number
            new_details, page_number = self._get_change_details(**kwargs)
            details.update(new_details)
        return details

    def _get_change_details(self, **kwargs):
        change_details = {}
        row_pattern = '(<table border=\"\d\"\scellpadding=\"\d\"\scellspacing=\"\d\"\swidth=\"[\d]+%\">)(?P<rows>[.\s\S]+?)(<\/table>)'
        url = _base_stem + _url_stems[kwargs['driving_id_type']]
        page_number = kwargs.get('page_number', 1)
        data = {
            kwargs['driving_id_name']: kwargs['driving_id_value'],
            'start_date': self._format_datetime_id_value(kwargs['local_max_value']),
            'changelogcategory_id': kwargs.get('category_id', ''),
            'changelogtype_id': kwargs.get('action_id', ''),
            'page': page_number
        }
        page_number += 1
        response = self._session.post(url, data=data)
        if response.status_code != 200:
            raise RuntimeError('could not retrieve change details for %s' % data)
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

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
    @_login_required
    def __get_change_details(self, changelog_id):
        changes = []
        url = _base_stem + _url_stems['ChangeDetail']
        response = self._session.get(url, params={'changelog_id': changelog_id})
        detail_soup = bs4.BeautifulSoup(response.content, features='html.parser')
        all_rows = detail_soup.find_all('tr')
        interesting_rows = all_rows[4:]
        for row in interesting_rows:
            changes.append({
                'id_name': str(row.contents[1].string).lower(),
                'old_value': str(row.contents[3].string),
                'new_value': str(row.contents[5].string)
            })
        return changes

    @classmethod
    def _format_datetime_id_value(cls, id_value):
        credible_format = '%m/%d/%Y'
        if id_value is None:
            id_value = datetime.datetime.now() - datetime.timedelta(days=3650)
        return id_value.strftime(credible_format)

    @classmethod
    def _format_datetime_value(cls, datetime_value):
        credible_format = '%m/%d/%Y %I:%M %p'
        if datetime_value is None:
            datetime_value = datetime.datetime.now() - datetime.timedelta(days=3650)
        return datetime_value.strftime(credible_format)
