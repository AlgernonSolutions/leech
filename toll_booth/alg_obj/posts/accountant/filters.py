class RecordMatchFailedException(Exception):
    def __init__(self, matched_type, matching_criteria, failure_type, duplicate_records=None):
        self._matched_type = matched_type
        self._matching_criteria = matching_criteria
        self._failure_type = failure_type
        self._duplicate_records = duplicate_records

    @property
    def matched_type(self):
        return self._matched_type

    @property
    def failure_type(self):
        return self._failure_type

    @property
    def duplicate_records(self):
        return self._duplicate_records


def filter_caseload_report(**kwargs):
    raise NotImplementedError()


def filter_psi_caseload_report(**kwargs):
    reports = {}
    missing = {'clients': [], 'providers': []}
    duplicates = []
    query_data = {x['query_name']: x['query_results'] for x in kwargs['query_data']}
    caseloads = query_data['caseloads']
    service_assessments = query_data['service_based_assessments']
    clients = query_data['clients']
    employees = query_data['employees']
    encounters = query_data['encounters']
    providers = {}
    caseloads = {x: y for x, y in caseloads.items() if y['Clinic Supervisor Name'] and y['CSW Name'] and y['Client Name']}
    for medicaid_number, caseload_entry in caseloads.items():
        patient_name = caseload_entry['Client Name']
        provider_name = caseload_entry['CSW Name']
        supervisor_name = caseload_entry['Clinic Supervisor Name']
        try:
            patient_id = _search_for_patient(medicaid_number, patient_name, clients)
            provider_id = _find_provider(provider_name, employees, providers)
            supervisor_id = _find_supervisor(supervisor_name, employees, providers)
        except RecordMatchFailedException as e:
            if e.failure_type == 'duplicates':
                duplicates.append({'provided': caseload_entry, 'found': e.duplicate_records})
                continue
            missing[e.matched_type].append(caseload_entry)
            continue
        providers[provider_name] = provider_id
        providers[supervisor_name] = supervisor_id
        if supervisor_id not in reports:
            reports[supervisor_id] = {}
        if provider_id not in reports:
            reports[provider_id] = {}
        patient_assessments = _filter_encounter_records(patient_id, service_assessments)
        patient_encounters = _filter_encounter_records(patient_id, encounters)
        patient_report = {
            'patient_id': patient_id,
            'patient_name': patient_name,
            'encounters': patient_encounters,
            'assessments': patient_assessments
        }
        provider_report = {patient_id: patient_report}
        supervisor_report = {provider_id: provider_report, 'provider_name': provider_name}
        reports[provider_id].update(provider_report)
        reports[supervisor_id].update(supervisor_report)
    raise NotImplementedError()


def _filter_encounter_records(patient_id, encounter_records):
    filtered_records = {}
    for record in encounter_records:
        record_patient_id = record['Consumer ID']
        if patient_id == record_patient_id:
            filtered_records[record['Service ID']] = record
    return filtered_records


def _search_for_patient(medicaid_number, patient_name, patient_records):
    try:
        last_name, first_name = patient_name.split(', ')
    except ValueError:
        first_name, last_name = patient_name.split(' ')
    matches = []
    for record in patient_records:
        record_last_name = record['Last Name']
        record_first_name = record['First Name']
        record_medicaid_number = record['Medicaid ID']
        if record_last_name is None or record_first_name is None:
            continue
        # if record_last_name.lower() == last_name.lower() and record_first_name.lower() == first_name.lower():
        if record_medicaid_number is None:
            if record_last_name.lower() == last_name.lower() and record_first_name.lower() == first_name.lower():
                matches.append(record)
            continue
        if medicaid_number in record_medicaid_number:
            if record_last_name.lower() == last_name.lower() or record_first_name.lower() == first_name.lower():
                matches.append(record)
            continue
    if len(matches) != 1:
        match_criteria = (first_name, last_name, medicaid_number)
        if len(matches) > 1:
            raise RecordMatchFailedException('clients', match_criteria, 'duplicates', matches)
        raise RecordMatchFailedException('clients', match_criteria, 'not found')
    for match in matches:
        return match[' Id']


def _find_provider(provider_name, employees, found_providers):
    provider_id = found_providers.get(provider_name, None)
    if provider_id is None:
        try:
            last_name, first_name = provider_name.split(', ')
        except ValueError:
            first_name, last_name = provider_name.split(' ')
        provider_id = _search_for_provider(last_name, first_name, employees)
    return provider_id


def _find_supervisor(supervisor_name, employees, found_providers):
    supervisor_id = found_providers.get(supervisor_name, None)
    if supervisor_id is None:
        first_name, last_name = supervisor_name.split(' ')
        supervisor_id = _search_for_provider(last_name, first_name, employees)
    return supervisor_id


def _search_for_provider(last_name, first_name, provider_records):
    matches = set()
    for record in provider_records:
        record_first_name = record['First Name']
        record_last_name = record['Last Name']
        if record_first_name is None or record_last_name is None:
            continue
        if record_first_name.lower() == first_name.lower() and record_last_name.lower() == last_name.lower():
            matches.add(record['Employee ID'])
    if len(matches) != 1:
        match_criteria = (first_name, last_name)
        if len(matches) > 1:
            raise RecordMatchFailedException('providers', match_criteria, 'duplicates')
        raise RecordMatchFailedException('providers', match_criteria, 'not found')
    for match in matches:
        return match
