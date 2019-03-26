from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('get_encounter_documentation')
@task('get_encounter_documentation')
def get_client_encounters(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver

    id_source = kwargs['id_source']
    client_search_kwargs = {

    }
    with CredibleFrontEndDriver(id_source) as driver:
        client_data = driver.process_advanced_search('Client', )


@xray_recorder.capture('get_encounter_documentation')
@task('get_encounter_documentation')
def get_client_encounter_documentation(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
    import re

    id_source = kwargs['id_source']
    encounter_ids = kwargs['encounter_ids']
    patterns = {
        'goal': re.compile('Goal:[\s]+(?P<target>.+)Start'),
        'objective': re.compile('Objective:[\s]+(?P<target>.+)Start'),
        'intervention': re.compile('Intervention:[\s]+(?P<target>.+)Start'),
        'description': re.compile('Description:[\s]+(?P<target>.+)')
    }
    encounters = {}
    with CredibleFrontEndDriver(id_source) as driver:
        for encounter_id in encounter_ids:
            raw_encounter = driver.retrieve_client_encounter(encounter_id)
            encounters[encounter_id] = _parse_encounter(raw_encounter, patterns)
    return {'encounters': encounters}


def _parse_encounter(raw_encounter, patterns):
    import bs4
    from toll_booth.alg_obj.forge.credible_specifics.dcdbh_specific.encounter import DcdbhDocumentation, DcdbhDocumentationEntry

    encounter_soup = bs4.BeautifulSoup(raw_encounter, features='html.parser')
    table_data = encounter_soup.find_all('td')
    table_text = [x.text for x in table_data]
    goals = [x for x in table_text if all(['Goal:' in x, 'Description:' in x, 'Progress Note' not in x])]
    objectives = [x for x in table_text if all(['Objective:' in x, 'Description:' in x, 'Progress Note' not in x])]
    interventions = [x for x in table_text if
                     all(['Intervention:' in x, 'Description:' in x, 'Progress Note' not in x])]
    documentations = []
    responses = []
    for index, entry in enumerate(table_text):
        if entry == 'Documentation':
            documentation_index = index + 2
            documentations.append(table_text[documentation_index])
        if entry == 'Response/Next Session':
            response_index = index + 5
            responses.append(table_text[response_index])
    goal_text = [(patterns['goal'].search(x).group('target'), patterns['description'].search(x).group('target')) for x
                 in goals]
    objective_text = [
        (patterns['objective'].search(x).group('target'), patterns['description'].search(x).group('target')) for x in
        objectives]
    intervention_text = [
        (patterns['intervention'].search(x).group('target'), patterns['description'].search(x).group('target')) for x in
        interventions]
    dcdbh_documentation = DcdbhDocumentation(response=responses)
    for _ in range(len(goal_text)):
        dcdbh_documentation.add_entry(
            DcdbhDocumentationEntry(goal_text[_], objective_text[_], intervention_text[_], documentations[_])
        )
    return dcdbh_documentation
