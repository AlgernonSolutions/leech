from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('work_encounter')
@task('work_encounter')
def work_encounter(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
    import bs4

    id_source = kwargs['id_source']
    encounter_id = kwargs['id_value']
    documentation_id = f'{encounter_id}-1'
    return_name = f'work_encounter-{encounter_id}'
    with CredibleFrontEndDriver(id_source) as driver:
        encounter = driver.retrieve_client_encounter(encounter_id)
        encounter_soup = bs4.BeautifulSoup(encounter, features="lxml")
        identifier_data = encounter_soup.find_all('a', {'href': '#'})
        encounter_type = identifier_data[0].text
        if encounter_type == 'Community Support':
            encounter_documentation = _parse_community_support_encounter(encounter_soup)
            return {return_name: [{
                'source': {
                    'documentation_entry_id': f'{documentation_id}-{x["entry_name"]}',
                    'documentation_id': documentation_id,
                    'encounter_id': encounter_id,
                    'entry_name': x['entry_name'],
                    'id_source': id_source,
                    'entry_value': x['entry_value']
                }
            } for x in encounter_documentation]}
    return


@xray_recorder.capture('retrieve_client_encounters')
@task('retrieve_client_encounters')
def retrieve_client_encounters(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver

    id_source = kwargs['id_source']
    client_id = kwargs['client_id']
    with CredibleFrontEndDriver(id_source) as driver:
        search_params = {
            'visittype_id': "29",
            'emp_int_id': 1,
            'client_int_id': 1,
            'clientvisit_id': 1,
            'timein': 1,
            'timeout': 1,
            'rev_timeout': 1,
            'visittype': 1,
            'data_dict_ids': [80, 81, 82, 87, 83, 84, 85, 86],
            'client_id': client_id
        }
        encounter_data = driver.process_advanced_search('ClientVisit', search_params)
        encounters = [{
            'encounter_id': x['Service ID'],
            'provider_id': x['Staff ID'],
            'patient_id': client_id,
            'encounter_type': x['Service Type'],
            'id_source': id_source,
            'encounter_start_time': x['Time In'],
            'encounter_end_time': x['Time Out'],
        } for x in encounter_data]
        return {'encounters': encounters, 'encounter_ids': [x['encounter_id'] for x in encounters]}


def _parse_community_support_encounter(encounter_soup):
    import re

    encounter_documentation = []
    patterns = {
        'goal': re.compile('Goal:[\s]+(?P<target>.+)Start'),
        'objective': re.compile('Objective:[\s]+(?P<target>.+)Start'),
        'intervention': re.compile('Intervention:[\s]+(?P<target>.+)Start'),
        'description': re.compile('Description:[\s]+(?P<target>.+)')
    }
    table_data = encounter_soup.find_all('td')
    table_text = [x.text for x in table_data]
    goals = [x for x in table_text if all(['Goal:' in x, 'Description:' in x, 'Progress Note' not in x, 'Objective:' not in x])]
    objectives = [x for x in table_text if all(['Objective:' in x, 'Description:' in x, 'Progress Note' not in x, 'Goal' not in x])]
    interventions = [x for x in table_text if
                     all(['Intervention:' in x, 'Description:' in x, 'Progress Note' not in x, 'Objective:' not in x])]
    documentations = []
    responses = []
    for index, entry in enumerate(table_text):
        if entry == 'Documentation':
            documentation_index = index + 2
            documentations.append(table_text[documentation_index])
        if entry == 'Response/Next Session':
            response_index = index + 5
            responses.append(table_text[response_index])

    goal_text = {(patterns['goal'].search(x).group('target'), patterns['description'].search(x).group('target')) for
                 x in goals}
    objective_text = {
        (patterns['objective'].search(x).group('target'), patterns['description'].search(x).group('target')) for x
        in objectives}
    intervention_text = {
        (patterns['intervention'].search(x).group('target'), patterns['description'].search(x).group('target')) for
        x in interventions}
    for text in [('goal', goal_text), ('objective', objective_text), ('intervention', intervention_text)]:
        entry_type = text[0]
        for pointer, entry in enumerate(text[1]):
            encounter_documentation.append({
                'entry_name': f'{entry_type}-{entry[0]}-{pointer}',
                'entry_value': entry[1]
            })
    return encounter_documentation
