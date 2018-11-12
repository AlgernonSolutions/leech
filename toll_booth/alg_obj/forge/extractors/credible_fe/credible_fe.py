from requests import cookies
import requests
import re
from toll_booth.alg_obj.aws.squirrels.squirrel import Opossum


class CredibleFrontEndLoginException(Exception):
    pass


class CredibleFrontEndDriver:
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
            raise exc_type(exc_val)
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

    def get_monitor_extraction(self, object_type):
        return

    def get_field_value_max_min(self, identifier_stem):
        return

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
    def _logout(cls, cookie_jar, session):
        logout_url = 'https://ww7.crediblebh.com/secure/logout.aspx'
        session.get(
            logout_url,
            cookies=cookie_jar
        )
