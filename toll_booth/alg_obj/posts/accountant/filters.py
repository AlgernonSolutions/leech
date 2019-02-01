def filter_caseload_report(**kwargs):
    raise NotImplementedError()


def filter_psi_caseload_report(**kwargs):
    query_data = {x['query_name']: x['query_results'] for x in kwargs['query_data']}
    caseloads = query_data['caseloads']
    service_assessments = query_data['service_based_assessments']
    clients = query_data['clients']
    employees = query_data['employees']
    encounters = query_data['encounters']
    raise NotImplementedError()
