import os
import re
from datetime import datetime, timedelta
from decimal import Decimal

import pytz
from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('get_productivity_report_data')
@task('get_productivity_report_data')
def get_productivity_report_data(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver

    today = datetime.utcnow()
    staff_search_data = {
        'emp_status_f': 'ACTIVE',
        'first_name': 1,
        'last_name': 1,
        'emp_id': 1,
        'profile_code': 1,
        'asgn_supervisors': 1,
        'asgn_supervisees': 1
    }
    client_search_data = {
        'teams': 1,
        'client_id': 1,
        'last_name': 1,
        'first_name': 1,
        'text28': 1,
        'dob': 1,
        'ssn': 1,
        'primary_assigned': 1,
        'client_status_f': 'ALL ACTIVE'
    }
    encounter_search_data = {
        'clientvisit_id': 1,
        'service_type': 1,
        'non_billable': 1,
        'consumer_name': 1,
        'staff_name': 1,
        'client_int_id': 1,
        'emp_int_id': 1,
        'non_billable1': 3,
        'visittype': 1,
        'orig_rate_amount': 1,
        'timein': 1
    }
    unapproved_search_data = encounter_search_data.copy()
    unapproved_search_data.update({
        'non_billable1': 0,
        'show_unappr': 1,
        'wh_fld1': 'cv.appr',
        'wh_cmp1': '=',
        'wh_val1': False,
        'data_dict_ids': 641
    })
    tx_plan_args = encounter_search_data.copy()
    tx_plan_args['visittype_id'] = 3
    da_args = encounter_search_data.copy()
    da_args['visittype_id'] = 5
    encounter_start_date = today - timedelta(days=90)
    unapproved_start_date = today - timedelta(days=365)

    id_source = kwargs['id_source']
    report_args = {
        'emp_data': ('Employees', staff_search_data),
        'client_data': ('Clients', client_search_data),
        'encounter_data': ('ClientVisit', encounter_search_data, encounter_start_date, today),
        'unapproved_data': ('ClientVisit', unapproved_search_data, unapproved_start_date, today),
        'tx_data': ('ClientVisit', tx_plan_args, unapproved_start_date, today),
        'da_data': ('ClientVisit', da_args, unapproved_start_date, today)
    }
    results = {}
    with CredibleFrontEndDriver(id_source) as driver:
        for report_name, report_arg in report_args.items():
            results[report_name] = driver.process_advanced_search(*report_arg)
    if not results:
        return
    return results


@xray_recorder.capture('build_daily_report')
@task('build_daily_report')
def build_daily_report(**kwargs):
    daily_report = {}
    encounter_data = kwargs['encounter_data']
    encounters = [{
        'clientvisit_id': int(x['Service ID']),
        'rev_timeout': x['Service Date'],
        'visit_type': x['Service Type'],
        'non_billable': bool(x['Non Billable']),
        'emp_id': int(x['Staff ID']),
        'client_id': int(x['Consumer ID']),
        'base_rate': Decimal(re.sub(r'[^\d.]', '', x['Base Rate']))
    } for x in encounter_data]
    unapproved_data = kwargs['unapproved_data']
    unapproved = [{
        'clientvisit_id': int(x['Service ID']),
        'rev_timeout': x['Service Date'],
        'visit_type': x['Service Type'],
        'non_billable': bool(x['Non Billable']),
        'emp_id': int(x['Staff ID']),
        'client_id': int(x['Consumer ID']),
        'red_x': x['Manual RedX Note'],
        'base_rate': Decimal(re.sub(r'[^\d.]', '', x['Base Rate']))
    } for x in unapproved_data]
    caseloads = kwargs['caseloads']
    for team_name, employees in caseloads.items():
        page_name = f'productivity_{team_name}'
        productivity_results = _build_team_productivity(employees, encounters, unapproved)
        daily_report[page_name] = productivity_results
    return {'report': daily_report}


@xray_recorder.capture('get_da_tx_data')
@task('get_da_tx_data')
def get_da_tx_data(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver

    id_source = kwargs['id_source']
    with CredibleFrontEndDriver(id_source) as driver:
        pass


@xray_recorder.capture('write_report_data')
@task('write_report_data')
def write_report_data(**kwargs):
    from openpyxl import Workbook
    from toll_booth.alg_obj.aws.snakes.invites import ObjectDownloadLink

    report_bucket_name = kwargs.get('report_bucket_name', os.getenv('REPORT_BUCKET_NAME', 'algernonsolutions-leech'))
    local_user_directory = os.path.expanduser('~')
    report_name = kwargs['report_name']
    id_source = kwargs['id_source']
    today = datetime.utcnow()
    today_string = today.strftime('%Y%m%d')
    report_name = f'{report_name}_{today_string}.xlsx'
    report_save_path = os.path.join(local_user_directory, report_name)
    report_data = kwargs['report_data']
    report_book = Workbook()
    front_sheet = report_book.active
    for entry_name, report_data in report_data.items():
        new_sheet = report_book.create_sheet(entry_name)
        new_sheet.append([entry_name])
        for row in report_data:
            new_sheet.append(row)
    report_book.save(report_save_path)
    download_link = ObjectDownloadLink(report_bucket_name, f'{id_source}/{report_name}', local_file_path=report_save_path)
    return {'download_link': download_link}


@xray_recorder.capture('build_clinical_teams')
@task('build_clinical_teams')
def build_clinical_teams(**kwargs):
    teams = kwargs['teams']
    manual_assignments = kwargs['manual_assignments']
    first_level = kwargs['first_level']
    default_team = kwargs['default_team']
    emp_data = kwargs['emp_data']

    for entry in emp_data:
        int_emp_id = int(entry['Employee ID'])
        str_emp_id = str(int_emp_id)
        supervisor_names = entry['Supervisors']
        profile_code = entry['profile_code']
        if supervisor_names is None or profile_code != 'CSA Community Support Licensed NonLicensed':
            continue
        emp_record = {
            'emp_id': int_emp_id,
            'first_name': entry['First Name'],
            'last_name': entry['Last Name'],
            'profile_code': entry['profile_code'],
            'caseload': []
        }
        if str_emp_id in manual_assignments:
            teams[manual_assignments[str_emp_id]].append(emp_record)
            continue
        for name in first_level:
            if name in supervisor_names:
                teams[name].append(emp_record)
                break
        teams[default_team].append(emp_record)
    return {'teams': teams}


@xray_recorder.capture('build_clinical_caseloads')
@task('build_clinical_caseloads')
def build_clinical_caseloads(**kwargs):
    caseloads = {'unassigned': []}
    name_lookup = {}
    teams = kwargs['teams']
    clients = kwargs['client_data']
    for team_name, employees in teams.items():
        if team_name not in caseloads:
            caseloads[team_name] = {}
        for employee in employees:
            emp_id = employee['emp_id']
            last_name = employee['last_name']
            first_name = employee['first_name']
            list_name = f'{first_name[1:]} {last_name}'
            name_lookup[list_name] = emp_id
            if emp_id not in caseloads[team_name]:
                caseloads[team_name][emp_id] = []
    for client in clients:
        client_id = client[' Id']
        primary_assigned = client['Primary Staff']
        if not primary_assigned:
            caseloads['unassigned'].append({
                'client_id': client_id,
                'last_name': client['Last Name'],
                'first_name': client['First Name'],
                'medicaid_id': client['Medicaid ID'],
                'dob': client['DOB'],
                'ssn': client['SSN'],
            })
        primary_names = _parse_staff_names(primary_assigned)
        client_record = {
            'client_id': client_id,
            'last_name': client['Last Name'],
            'first_name': client['First Name'],
            'medicaid_id': client['Medicaid ID'],
            'dob': client['DOB'],
            'ssn': client['SSN'],
            'primary_staff': primary_names
        }
        for staff_name in primary_names:
            found_emp_id = name_lookup.get(staff_name)
            if found_emp_id:
                for team_name, employees in caseloads.items():
                    for emp_id, employee in employees.items():
                        if emp_id == found_emp_id:
                            employee['caseload'].append(client_record)
    return {'caseloads': caseloads}


def _parse_staff_names(primary_staff_line):
    staff = []
    program_pattern = '(\([^)]*\))'
    program_re = re.compile(program_pattern)
    staff_names = primary_staff_line.split(', ')
    for name in staff_names:
        program_match = program_re.search(name)
        if program_match:
            program_name = program_match.group(1)
            name = name.replace(program_name, '')
        staff.append(name)
    return staff


def _build_team_productivity(team_caseload, encounters, unapproved):
    results = []
    twenty_four_hours_ago = datetime.now(pytz.timezone('US/Eastern')) - timedelta(hours=24)
    six_days_ago = datetime.now(pytz.timezone('US/Eastern')) - timedelta(days=6)
    past_day_encounters = [x for x in encounters if encounters['rev_timeout'] <= twenty_four_hours_ago]
    next_six_days_encounters = [x for x in encounters if all(
        [encounters['rev_timeout'] > twenty_four_hours_ago, encounters['rev_timeout'] <= six_days_ago]
    )]
    for emp_id, employee in team_caseload.items():
        emp_past_day_encounters = [x['base_rate'] for x in past_day_encounters if x['emp_id'] == emp_id]
        emp_next_six_days_encounters = [x['base_rate'] for x in next_six_days_encounters if x['emp_id'] == emp_id]
        emp_red_x = [x['base_rate'] for x in unapproved if x['emp_id'] == emp_id and x['red_x']]
        emp_unapproved = [x['base_rate'] for x in unapproved if x['emp_id'] == emp_id and not x['red_x']]
        results.append([
            emp_id, f'{employee["last_name"]}, {employee["first_name"]}',
            sum(emp_past_day_encounters), sum(emp_next_six_days_encounters), sum(emp_unapproved), sum(emp_red_x)
        ])
    return results
