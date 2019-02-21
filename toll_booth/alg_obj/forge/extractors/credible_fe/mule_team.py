import datetime
import logging
import re
import threading
import time
from collections import deque
from decimal import Decimal
from queue import Queue

import bs4
import pytz

from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
from toll_booth.alg_obj.forge.extractors.credible_fe.base_stems import BASE_STEM, URL_STEMS
from toll_booth.alg_obj.forge.extractors.credible_fe.cache import CachedEmployeeIds
from toll_booth.alg_obj.forge.extractors.credible_fe.credible_csv_parser import CredibleCsvParser
from toll_booth.alg_obj.forge.extractors.credible_fe.regex_patterns import ROW_PATTERN, NAME_PATTERN


class CredibleMuleTeam:
    def __init__(self, id_source, **kwargs):
        self._id_source = id_source
        self._thread_count = kwargs.get('thread_count', 5)
        self._workers = []
        self._work = Queue()
        self._driver_work = Queue()
        self._driver_results = {x: Queue() for x in range(self._thread_count)}
        self._results = deque()
        self._worker_results = Queue()
        self._row_pattern = re.compile(ROW_PATTERN)
        self._name_pattern = re.compile(NAME_PATTERN)
        self._cached_emp_ids = kwargs.get('cached_emp_ids', CachedEmployeeIds())
        self._score_board = {}

    def _start_threads(self):
        for worker_id in range(self._thread_count):
            t = threading.Thread(target=self._lash_mules)
            t.start()
            self._workers.append(t)
        driver_thread = threading.Thread(target=self._lash_driver)
        driver_thread.start()
        self._workers.append(driver_thread)

    def _stop_threads(self):
        logging.debug('called to stop the threads in the mule team')
        for worker_id in range(self._thread_count):
            self._work.put(None)
        self._driver_work.put(None)
        for worker in self._workers:
            worker.join()

    def enrich_data(self, **kwargs):
        enrichment = {}
        self._enrich_data(**kwargs)
        self._start_threads()
        while self._score_board:
            logging.debug(self._score_board)
            time.sleep(3)
        self._stop_threads()
        for result in self._results:
            for entry_name, entry in result.items():
                if entry_name not in enrichment:
                    enrichment[entry_name] = {}
                enrichment[entry_name].update(entry)
        return enrichment

    def _enrich_data(self, **kwargs):
        self._work.put({
            'fn_name': '_extract_changelog_page',
            'fn_kwargs': kwargs
        })

    def _lash_driver(self):
        logging.debug('starting the threaded driver')
        with CredibleFrontEndDriver(self._id_source) as driver:
            logging.debug('got the tokens, going to work with the threaded driver')
            while True:
                assignment = self._driver_work.get()
                if assignment is None:
                    logging.debug('work is done, credible driver leaving')
                    return
                function_name = assignment['fn_name']
                function_kwargs = assignment['fn_kwargs']
                logging.debug('the credible driver got an assignment: %s' % function_name)
                function_kwargs['driver'] = driver
                getattr(self, function_name)(**function_kwargs)
                logging.debug('the credible driver finished an assignment: %s' % function_name)
                self._driver_work.task_done()

    def _lash_mules(self):
        logging.debug('started a mule for the mule train')
        while True:
            assignment = self._work.get()
            if assignment is None:
                logging.debug('work is done, mule is leaving')
                return
            function_name = assignment['fn_name']
            function_kwargs = assignment['fn_kwargs']
            getattr(self, function_name)(**function_kwargs)
            self._work.task_done()

    def _extract_changelog_page(self, **kwargs):
        url = BASE_STEM + URL_STEMS[kwargs['driving_id_type']]
        page_number = kwargs.get('page_number', 1)
        extract_kwargs = kwargs.copy()
        extract_kwargs['url'] = url
        self._driver_work.put({
            'fn_name': '_get_changelog_page',
            'fn_kwargs': extract_kwargs
        })
        task_name = f'extract_changelog_page_{page_number}'
        self._score_board[task_name] = False

    def _parse_changelog_page(self, **kwargs):
        page_number = kwargs.get('page_number', 1)
        response = kwargs['results']
        row_match = self._row_pattern.search(response).group('rows')
        row_soup = bs4.BeautifulSoup(row_match, features='html.parser')
        table_rows = row_soup.find_all('tr')
        if kwargs['get_emp_ids']:
            emp_id_kwargs = kwargs.copy()
            emp_id_kwargs['results'] = table_rows
            self._work.put({
                'fn_name': '_strain_emp_ids',
                'fn_kwargs': emp_id_kwargs
            })
            emp_task_name = f'strain_emp_ids_{page_number}'
            self._score_board[emp_task_name] = False
        if kwargs['get_details']:
            details_kwargs = kwargs.copy()
            details_kwargs['results'] = row_soup
            self._work.put({
                'fn_name': '_strain_change_details',
                'fn_kwargs': details_kwargs
            })
            detail_task_name = f'strain_change_details_{page_number}'
            self._score_board[detail_task_name] = False
        task_name = f'extract_changelog_page_{page_number}'
        del (self._score_board[task_name])
        if len(table_rows) <= 3:
            page_number = None
        if page_number:
            page_number += 1
            extract_kwargs = kwargs.copy()
            extract_kwargs['page_number'] = page_number
            del(extract_kwargs['results'])
            self._work.put({
                'fn_name': '_extract_changelog_page',
                'fn_kwargs': extract_kwargs
            })

    def _strain_emp_ids(self, **kwargs):
        table_rows = kwargs['results']
        for row in table_rows:
            if 'style' in row.attrs:
                utc_change_date_string = row.contents[15].string
                try:
                    change_date_utc = datetime.datetime.strptime(utc_change_date_string, '%m/%d/%Y %I:%M:%S %p')
                except ValueError:
                    utc_change_date_string = f'{utc_change_date_string} 12:00:00 AM'
                    change_date_utc = datetime.datetime.strptime(utc_change_date_string, '%m/%d/%Y %I:%M:%S %p')
                change_date_utc = change_date_utc.replace(tzinfo=pytz.UTC)
                utc_timestamp = change_date_utc.timestamp()
                done_by_entry = row.contents[9]
                done_by_name = done_by_entry.string
                try:
                    matches = self._name_pattern.search(done_by_name)
                except TypeError:
                    raise RuntimeError()
                last_name = matches.group('last_name')
                first_initial = matches.group('first_initial')
                emp_id = self._cached_emp_ids.get_emp_id(last_name, first_initial, utc_timestamp)
                if emp_id is None:
                    emp_id_kwargs = kwargs.copy()
                    del(emp_id_kwargs['results'])
                    self._cached_emp_ids.mark_emp_id_working(last_name, first_initial, utc_timestamp)
                    emp_id_kwargs['last_name'] = last_name
                    emp_id_kwargs['first_initial'] = first_initial
                    emp_id_kwargs['change_date_utc'] = utc_timestamp
                    logging.debug('calling for a search employees operation')
                    self._work.put({
                        'fn_name': '_search_employees',
                        'fn_kwargs': emp_id_kwargs
                    })
                    continue
                self._results.append({'emp_ids': {utc_timestamp: emp_id}})
        page_number = kwargs.get('page_number', 1)
        emp_task_name = f'strain_emp_ids_{page_number}'
        del(self._score_board[emp_task_name])

    def _strain_change_details(self, **kwargs):
        row_soup = kwargs['results']
        table_rows = row_soup.find_all('a')
        if len(table_rows) <= 6:
            return {}
        for row in table_rows:
            if 'clid' in row.attrs:
                detail_kwargs = kwargs.copy()
                changelog_id = row.attrs['clid']
                # changelog_id = re.compile('changelog_id=(?P<changelog_id>\d+)').search(target).group('changelog_id')
                detail_kwargs['changelog_id'] = changelog_id
                del(detail_kwargs['results'])
                self._work.put({
                    'fn_name': '_get_change_details',
                    'fn_kwargs': detail_kwargs
                })
                containing_row = row.parent.parent
                utc_change_date_string = containing_row.contents[15].string
                try:
                    change_date_utc = datetime.datetime.strptime(utc_change_date_string, '%m/%d/%Y %I:%M:%S %p')
                except ValueError:
                    utc_change_date_string = f'{utc_change_date_string} 12:00:00 AM'
                    change_date_utc = datetime.datetime.strptime(utc_change_date_string, '%m/%d/%Y %I:%M:%S %p')
                change_date_utc = change_date_utc.replace(tzinfo=pytz.UTC)
                self._results.append({
                    'change_detail': {changelog_id: change_date_utc.timestamp()}
                })
        page_number = kwargs.get('page_number', 1)
        task_name = f'strain_change_details_{page_number}'
        del (self._score_board[task_name])

    def _search_employees(self, **kwargs):
        logging.debug('started a search employees operation')
        url = BASE_STEM + URL_STEMS['Employee Advanced']
        search_kwargs = kwargs.copy()
        search_kwargs['url'] = url
        self._driver_work.put({
            'fn_name': '_get_emp_id_search',
            'fn_kwargs': search_kwargs
        })
        task_name = f'search_employees_for_{kwargs["last_name"]}_{kwargs["first_initial"]}'
        self._score_board[task_name] = False

    def _parse_emp_id_search(self, **kwargs):
        results = kwargs['results']
        try:
            possible_employees = CredibleCsvParser.parse_csv_response(results)
        except RuntimeError:
            possible_employees = []
        emp_id = None
        if len(possible_employees) == 1:
            for entry in possible_employees:
                for cell_name, row in entry.items():
                    if 'ID' in cell_name:
                        emp_id = Decimal(row)
        if emp_id is None:
            logging.warning('could not determine the emp_id from their name: %s' %
                            f'{kwargs["last_name"]}, {kwargs["first_initial"]}, using default value of 0')
            emp_id = 0
        self._cached_emp_ids.add_emp_id(kwargs['last_name'], kwargs['first_initial'], emp_id)
        self._results.append({'emp_ids': {kwargs['change_date_utc']: emp_id}})
        task_name = f'search_employees_for_{kwargs["last_name"]}_{kwargs["first_initial"]}'
        del(self._score_board[task_name])
        logging.debug('completed a search employees operation')

    def _get_change_details(self, **kwargs):
        logging.debug('started a get change details operation')
        url = BASE_STEM + URL_STEMS['ChangeDetail']
        detail_kwargs = kwargs.copy()
        detail_kwargs['url'] = url
        self._driver_work.put({
            'fn_name': '_get_change_detail_page',
            'fn_kwargs': detail_kwargs
        })
        task_name = f'get_change_detail_{kwargs["changelog_id"]}'
        self._score_board[task_name] = False

    def _parse_change_details(self, **kwargs):
        results = kwargs['results']
        detail_soup = bs4.BeautifulSoup(results, features='html.parser')
        all_rows = detail_soup.find_all('tr')
        interesting_rows = all_rows[4:]
        for row in interesting_rows:
            self._results.append({
                'change_details': {
                    kwargs['changelog_id']: {
                        'id_name': str(row.contents[1].string).lower(),
                        'old_value': str(row.contents[3].string),
                        'new_value': str(row.contents[5].string)
                    }
                }
            })
        task_name = f'get_change_detail_{kwargs["changelog_id"]}'
        del (self._score_board[task_name])
        logging.debug('completed a get change detail operation')

    def _get_changelog_page(self, url, **kwargs):
        logging.debug('the driver is retrieving a changelog page: %s' % kwargs)
        driver = kwargs['driver']
        del(kwargs['driver'])
        results = driver.retrieve_changelog_page(url, **kwargs)
        logging.debug('the driver retrieved a changelog page: %s' % kwargs)
        parse_kwargs = kwargs.copy()
        parse_kwargs['results'] = results
        self._work.put({
            'fn_name': '_parse_changelog_page',
            'fn_kwargs': parse_kwargs
        })

    def _get_change_detail_page(self, url, **kwargs):
        logging.debug('the driver is retrieving a change detail page: %s' % kwargs)
        changelog_id = kwargs['changelog_id']
        driver = kwargs['driver']
        del(kwargs['driver'])
        results = driver.retrieve_change_detail_page(url, changelog_id)
        detail_kwargs = kwargs.copy()
        detail_kwargs['results'] = results
        self._work.put({
            'fn_name': '_parse_change_details',
            'fn_kwargs': detail_kwargs
        })
        logging.debug('the driver retrieved a change detail page: %s' % kwargs)

    def _get_emp_id_search(self, url, **kwargs):
        logging.debug('the driver is retrieving a emp search page: %s' % kwargs)
        driver = kwargs['driver']
        del[kwargs['driver']]
        last_name, first_initial = kwargs['last_name'], kwargs['first_initial']
        results = driver.retrieve_emp_id_search(url, last_name, first_initial)
        emp_kwargs = kwargs.copy()
        emp_kwargs['results'] = results
        self._work.put({
            'fn_name': '_parse_emp_id_search',
            'fn_kwargs': emp_kwargs
        })
        logging.debug('the driver retrieved a emp search page: %s' % kwargs)
