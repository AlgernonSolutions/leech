from datetime import timedelta, datetime

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('get_productivity_report_data')
@task('get_productivity_report_data')
def get_productivity_report_data(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
    from datetime import datetime, timedelta

    credible_date_format = '%m/%d/%Y'

    today = datetime.utcnow()
    encounter_start_date = today - timedelta(days=90)
    unapproved_start_date = today - timedelta(days=365)
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
        'timein': 1,
        'wh_fld1': 'cv.transfer_date',
        'wh_cmp1': '>=',
        'wh_val1': encounter_start_date.strftime(credible_date_format),
        'data_dict_ids': 83
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

    id_source = kwargs['id_source']
    report_args = {
        'emp_data': ('Employees', staff_search_data),
        'client_data': ('Clients', client_search_data),
        'encounter_data': ('ClientVisit', encounter_search_data),
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
    import re
    from decimal import Decimal

    daily_report = {}
    encounter_data = kwargs['encounter_data']
    encounters = [{
        'clientvisit_id': int(x['Service ID']),
        'transfer_date': x['Transfer Date'],
        'visit_type': x['Service Type'],
        'non_billable': bool(x['Non Billable']),
        'emp_id': int(x['Staff ID']),
        'client_id': int(x['Consumer ID']),
        'base_rate': Decimal(re.sub(r'[^\d.]', '', x['Base Rate'])),
        'data_dict_ids': 83
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
    tx_plan_data = kwargs['tx_data']
    tx_plans = [{
        'rev_timeout': x['Service Date'],
        'emp_id':  int(x['Staff ID']),
        'client_id': int(x['Consumer ID'])
    } for x in tx_plan_data]
    da_data = kwargs['da_data']
    diagnostics = [{
        'rev_timeout': x['Service Date'],
        'emp_id': int(x['Staff ID']),
        'client_id': int(x['Consumer ID'])
    } for x in da_data]
    caseloads = kwargs['caseloads']
    for team_name, employees in caseloads.items():
        if team_name == 'unassigned':
            continue
        page_name = f'productivity_{team_name}'
        productivity_results = _build_team_productivity(employees, encounters, unapproved)
        daily_report[page_name] = productivity_results
    tx_report = _build_expiration_report(caseloads, tx_plans, 180)
    da_report = _build_expiration_report(caseloads, diagnostics, 180)
    unassigned_report = _build_unassigned_report(caseloads)
    daily_report['tx_plans'] = tx_report
    daily_report['diagnostics'] = da_report
    daily_report['unassigned'] = unassigned_report
    return {'report_data': daily_report}


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
    from openpyxl.styles import Font

    from toll_booth.alg_obj.aws.snakes.invites import ObjectDownloadLink
    import os
    from datetime import datetime

    report_bucket_name = kwargs.get('report_bucket_name', os.getenv('REPORT_BUCKET_NAME', 'algernonsolutions-leech'))
    local_user_directory = os.path.expanduser('~')
    report_name = kwargs['report_name']
    id_source = kwargs['id_source']
    today = datetime.utcnow()
    today_string = today.strftime('%Y%m%d')
    report_name = f'{report_name}_{today_string}.xlsx'
    report_save_path = os.path.join(local_user_directory, report_name)
    reports = kwargs['report_data']
    report_book = Workbook()
    front_sheet = report_book.active
    for entry_name, report_data in reports.items():
        new_sheet = report_book.create_sheet(entry_name)
        new_sheet.append([entry_name])
        top_row = new_sheet.row_dimensions[1]
        top_row.font = Font(bold=True, size=18)
        new_sheet.append([])
        row_lengths = [len(x) for x in report_data]
        max_row_length = None
        if row_lengths:
            max_row_length = max([len(x) for x in report_data])
        for row in report_data:
            new_sheet.append(row)
        if max_row_length:
            new_sheet.merge_cells(f'A1:{chr(ord("a") + (max_row_length-1))}1')
    report_book.remove(front_sheet)
    try:
        report_book.save(report_save_path)
    except FileNotFoundError:
        report_save_path = f'/tmp/{report_name}'
        report_book.save(report_save_path)
    download_link = ObjectDownloadLink(report_bucket_name, f'{id_source}/{report_name}', local_file_path=report_save_path)
    return {'download_link': download_link}


@xray_recorder.capture('send_report')
@task('send_report')
def send_report(**kwargs):
    download_link = kwargs['download_link']
    subject_line = 'Algernon Solutions Clinical Intelligence Report'
    text_body = f''' 
        You are receiving this email because you have requested to have routine reports sent to you through the Algernon Clinical Intelligence Platform. 
        The requested report can be downloaded from the included link. To secure the information contained within, the link will expire in {download_link.expiration_hours} hours.
        I hope this report brings you joy and the everlasting delights of a cold data set.

        {str(download_link)}

        - Algernon

        This communication, download link, and any attachment may contain information, which is sensitive, confidential and/or privileged, covered under HIPAA and is intended for use only by the addressee(s) indicated above. If you are not the intended recipient, please be advised that any disclosure, copying, distribution, or use of the contents of this information is strictly prohibited. If you have received this communication in error, please notify the sender immediately and destroy all copies of the original transmission.
    '''
    html_body = f'''
       <!DOCTYPE html>
        <html lang="en" xmlns="http://www.w3.org/1999/html" xmlns="http://www.w3.org/1999/html">
            <head>
                <meta charset="UTF-8">
                <title>Algernon Clinical Intelligence Report</title>
                <style>
                   
                    .container {{
                        position: relative;
                        text-align: center;
                    }}
                </style>
            </head>
            <body>
                <p>You are receiving this email because you have requested to have routine reports sent to you through the Algernon Clinical Intelligence Platform.</p>
                <p>The requested report can be downloaded from the included link. To secure the information contained within, the link will expire in {download_link.expiration_hours} hours.</p>
                <p>I hope this report brings you joy and the everlasting delights of a cold data set.</p>
                <h4><a href="{str(download_link)}">Download Report</a></h4>
                <p> - Algernon </p>
                <h5>
                    This communication, download link, and any attachment may contain information, which is sensitive, confidential and/or privileged, covered under HIPAA and is intended for use only by the addressee(s) indicated above.<br/>
                    If you are not the intended recipient, please be advised that any disclosure, copying, distribution, or use of the contents of this information is strictly prohibited.<br/>
                    If you have received this communication in error, please notify the sender immediately and destroy all copies of the original transmission.<br/>
                </h5>
            </body>
        </html>
    '''

    response = _send_by_ses(subject_line=subject_line, text_body=text_body, html_body=html_body, **kwargs)
    return {'message_id': response['MessageId'], 'text_body': text_body, 'html_body': html_body}


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
    caseloads = {}
    unassigned = []
    name_lookup = {}
    teams = kwargs['teams']
    clients = kwargs['client_data']
    for team_name, employees in teams.items():
        if team_name not in caseloads:
            caseloads[team_name] = {}
        for employee in employees:
            emp_id = str(employee['emp_id'])
            last_name = employee['last_name']
            first_name = employee['first_name']
            list_name = f'{first_name[0]} {last_name}'
            name_lookup[list_name] = emp_id
            if emp_id not in caseloads[team_name]:
                caseloads[team_name][emp_id] = employee
    for client in clients:
        client_id = client[' Id']
        primary_assigned = client['Primary Staff']
        if not primary_assigned:
            unassigned.append({
                'client_id': client_id,
                'last_name': client['Last Name'],
                'first_name': client['First Name'],
                'medicaid_id': client['Medicaid ID'],
                'dob': client['DOB'],
                'ssn': client['SSN'],
                'team': client['CSA (Team)']
            })
            continue
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
        found = False
        for staff_name in primary_names:
            found_emp_id = name_lookup.get(staff_name)
            if found_emp_id:
                for team_name, employees in caseloads.items():
                    for emp_id, employee in employees.items():
                        if emp_id == found_emp_id:
                            employee['caseload'].append(client_record)
                            found = True
        if not found:
            unassigned.append({
                'client_id': client_id,
                'last_name': client['Last Name'],
                'first_name': client['First Name'],
                'medicaid_id': client['Medicaid ID'],
                'dob': client['DOB'],
                'ssn': client['SSN'],
                'team': client['CSA (Team)'],
                'primary_staff': primary_assigned
            })
    client_ids = set()
    for team_name, team in caseloads.items():
        for emp_id, employee in team.items():
            client_ids.update([x['client_id'] for x in employee['caseload']])
    client_ids.update([x['client_id'] for x in unassigned])
    if client_ids - set([str(x[' Id']) for x in clients]):
        raise RuntimeError('while creating caseloads, we seemed to have missed someone, can not continue due to prime directive')
    caseloads['unassigned'] = unassigned
    return {'caseloads': caseloads}


def _parse_staff_names(primary_staff_line):
    import re

    staff = []
    if not primary_staff_line:
        return staff
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
    from datetime import datetime, timedelta

    results = [['CSW ID', 'CSW Name', 'Past 24 Hours', 'Next Past 6 Days', 'All Unapproved', 'All Red X Notes']]
    twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
    six_days_ago = datetime.now() - timedelta(days=6)
    past_day_encounters = [x for x in encounters if x['transfer_date'] >= twenty_four_hours_ago]
    next_six_days_encounters = [x for x in encounters if all(
        [x['transfer_date'] < twenty_four_hours_ago, x['transfer_date'] >= six_days_ago]
    )]
    for emp_id, employee in team_caseload.items():
        emp_id = int(emp_id)
        emp_past_day_encounters = [x['base_rate'] for x in past_day_encounters if x['emp_id'] == emp_id]
        emp_next_six_days_encounters = [x['base_rate'] for x in next_six_days_encounters if x['emp_id'] == emp_id]
        emp_red_x = [x['base_rate'] for x in unapproved if x['emp_id'] == emp_id and x['red_x']]
        emp_unapproved = [x['base_rate'] for x in unapproved if x['emp_id'] == emp_id and not x['red_x']]
        results.append([
            emp_id, f'{employee["last_name"]}, {employee["first_name"]}',
            sum(emp_past_day_encounters), sum(emp_next_six_days_encounters), sum(emp_unapproved), sum(emp_red_x)
        ])
    return results


def _build_expiration_report(caseloads, assessment_data, assessment_lifespan):
    lifespan_delta = timedelta(days=assessment_lifespan)
    inverted = _invert_caseloads(caseloads)
    now = datetime.now()
    max_assessments = {}
    results = [['Team', 'CSW Name', 'Start Date', 'End Date', 'Expired', 'Days Remaining']]
    for assessment in assessment_data:
        client_id = str(assessment['client_id'])
        if client_id not in max_assessments:
            max_assessments[client_id] = []
        max_assessments[client_id].append(assessment['rev_timeout'])
    for client_id, assessments in max_assessments.items():
        assignments = inverted.get(client_id, {'team': 'unassigned', 'csw': 'unassigned'})
        team_name, csw_name = assignments['team'], assignments['csw']
        max_assessment_date = max(assessments)
        expiration_date = max_assessment_date + lifespan_delta
        expired = False
        days_left = (expiration_date - now).days
        if expiration_date < now:
            expired = True
            days_left = 0
        results.append([team_name, csw_name, max_assessment_date, expiration_date, expired, days_left])
    no_assessments = set(inverted.keys()) - set(max_assessments.keys())
    for client_id in no_assessments:
        assignments = inverted.get(client_id, {'team': 'unassigned', 'csw': 'unassigned'})
        team_name, csw_name = assignments['team'], assignments['csw']
        results.append([team_name, csw_name, '',  '', True, 0])
    return results


def _build_unassigned_report(caseloads):
    report = [['Client ID', 'Client Name', 'DOB', 'SSN', 'Medicaid Number', 'Primary Assigned Staff', 'Assigned CSA']]
    for client in caseloads['unassigned']:
        report.append([
            client['client_id'], f'{client["last_name"]}, {client["first_name"]}',
            client['dob'], client['dob'], client['ssn'], client['team'], ', '.join(client.get('primary_staff', []))
        ])
    return report


def _send_by_ses(**kwargs):
    import boto3

    client = boto3.client('ses')
    recipients = kwargs['recipients']
    html_body, text_body = kwargs['html_body'], kwargs['text_body']
    subject_line = kwargs['subject_line']

    response = client.send_email(
        Source='algernon@algernon.solutions',
        Destination={
            'ToAddresses': [x['email_address'] for x in recipients]
        },
        Message={
            'Subject': {'Data': subject_line},
            'Body': {
                'Text': {'Data': text_body},
                'Html': {'Data': html_body}
            }
        },
        ReplyToAddresses=['algernon@algernon.solutions']
    )
    return response


def _send_by_pinpoint(**kwargs):
    import os
    import boto3

    client = boto3.client('pinpoint')
    application_id = kwargs.get('application_id', os.getenv('PINPOINT_APP_ID', 'e077e1ece06a40d983a1fb0cdeb76854'))
    recipients = kwargs['recipients']
    html_body, text_body = kwargs['html_body'], kwargs['text_body']
    subject_line = kwargs['subject_line']
    addresses = {
        x['email_address']: {'ChannelType': 'EMAIL'} for x in recipients
    }

    response = client.send_messages(
        ApplicationId=application_id,
        MessageRequest={
            'Addresses': addresses,
            'MessageConfiguration': {
                'EmailMessage': {
                    'FromAddress': 'algernon@algernon.solutions',
                    'ReplyToAddresses': [
                        'algernon@algernon.solutions',
                    ],
                    'SimpleEmail': {
                        'HtmlPart': {
                            'Data': html_body
                        },
                        'Subject': {
                            'Data': subject_line
                        },
                        'TextPart': {
                            'Data': text_body
                        }
                    }
                }
            }
        }
    )
    return response


def _invert_caseloads(caseloads):
    inverted_caseloads = {}
    for team_name, team_caseload in caseloads.items():
        if team_name == 'unassigned':
            for client in team_caseload:
                inverted_caseloads[client['client_id']] = {'team': 'unassigned', 'csw': 'unassigned'}
            continue
        for emp_id, employee in team_caseload.items():
            csw = f'{employee["last_name"]}, {employee["first_name"]}'
            for client in employee['caseload']:
                inverted_caseloads[client['client_id']] = {'team': team_name, 'csw': csw}
    return inverted_caseloads
