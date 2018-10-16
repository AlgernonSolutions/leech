def derive_change_targeted(**kwargs):
    extracted_data = kwargs['extracted_data']
    specifier = kwargs['specifier']
    target_constants = kwargs['target_constants']
    change_targeted = []
    specifier_data = extracted_data[specifier.specifier_name]
    for entry in specifier_data:
        if entry.get('client_id', None):
            target_constants.update({
                'id_type': 'Clients',
                'id_name': 'client_id',
                'id_value': entry['client_id']
            })
            change_targeted.append(target_constants)
        if entry.get('clientvisit_id', None):
            target_constants.update({
                'id_type': 'ClientVisit',
                'id_name': 'clientvisit_id',
                'id_value': entry['clientvisit_id']
            })
            change_targeted.append(target_constants)
        if entry.get('emp_id', None):
            target_constants.update({
                'id_type': 'Employees',
                'id_name': 'emp_id',
                'id_value': entry['emp_id']
            })
            change_targeted.append(target_constants)
        if entry.get('record_id', None):
            target_constants.update({
                'id_type': entry['record_type'],
                'id_name': entry['primarykey_name'],
                'id_value': entry['record_id']
            })
            change_targeted.append(target_constants)
    return change_targeted
