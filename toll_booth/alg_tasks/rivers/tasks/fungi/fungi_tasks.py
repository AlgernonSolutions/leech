import logging
from decimal import Decimal, InvalidOperation

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.forge.credible_specifics import FungalStem
from toll_booth.alg_obj.graph.index_manager.indexes import EmptyIndexException
from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('get_local_max_change_type_value')
@task('get_local_max_change_type_value')
def get_local_max_change_type_value(**kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
    from toll_booth.alg_obj.graph.index_manager.index_manager import IndexManager

    driving_identifier_stem = kwargs['driving_identifier_stem']
    id_value = kwargs['id_value']
    category_id = kwargs['category_id']
    changelog_types = kwargs['changelog_types']
    driving_identifier_stem = IdentifierStem.from_raw(driving_identifier_stem)
    id_source = driving_identifier_stem.get('id_source')
    id_type = driving_identifier_stem.get('id_type')
    change_category = changelog_types.categories[category_id]
    change_stem = f'#{id_source}#{id_type}#{id_value}#{change_category}'
    index_manager = IndexManager.from_graph_schema(kwargs['schema'], **kwargs)
    try:
        local_max_value = index_manager.query_object_max(change_stem)
    except EmptyIndexException:
        local_max_value = None
    return {'local_max_value': local_max_value}


@xray_recorder.capture('pull_change_types')
@task('pull_change_types')
def pull_change_types(**kwargs):
    from toll_booth.alg_obj.forge.credible_specifics import ChangeTypes

    change_types = ChangeTypes.retrieve(**kwargs)
    return {'changelog_types': change_types}


@xray_recorder.capture('unlink_old_id')
@task('unlink_old_id')
def unlink_old_id(**kwargs):
    tools, index_manager = _generate_link_data(**kwargs)
    tools['unlink'] = True
    index_manager.add_link_event(**tools)


@xray_recorder.capture('unlink_old_ids')
@task('unlink_old_ids')
def unlink_old_ids(**kwargs):
    tools, index_manager = _generate_link_data(**kwargs)
    tools['unlink'] = True
    new_unlinks = index_manager.add_link_events(**tools)
    return {'new_unlinks': new_unlinks}


@xray_recorder.capture('link_new_id')
@task('link_new_id')
def link_new_id(**kwargs):
    tools, index_manager = _generate_link_data(**kwargs)
    tools['link'] = True
    index_manager.add_link_event(**tools)


@xray_recorder.capture('link_new_ids')
@task('link_new_ids')
def link_new_ids(**kwargs):
    tools, index_manager = _generate_link_data(**kwargs)
    tools['link'] = True
    new_link_entries = index_manager.add_link_events(**tools)
    return {'new_links': new_link_entries}


@xray_recorder.capture('put_new_id')
@task('put_new_id')
def put_new_id(**kwargs):
    tools, index_manager = _generate_link_data(**kwargs)
    tools['put'] = True
    index_manager.add_link_event(**tools)


@xray_recorder.capture('put_new_ids')
@task('put_new_ids')
def put_new_ids(**kwargs):
    tools, index_manager = _generate_link_data(**kwargs)
    tools['put'] = True
    new_objects = index_manager.add_link_events(**tools)
    return {'new_link_histories': new_objects}


@xray_recorder.capture('graph_links')
@task('graph_links')
def graph_links(**kwargs):
    from toll_booth.alg_obj.graph.ogm.ogm import Ogm
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
    from toll_booth.alg_obj.graph import InternalId
    from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex

    edges = []
    driving_identifier_stem = IdentifierStem.from_raw(kwargs['identifier_stem'])
    id_source = driving_identifier_stem.get('id_source')
    internal_id = InternalId(''.join(['IdSource', id_source])).id_value
    identifier_stem = IdentifierStem('vertex', 'IdSource', {'id_source': id_source})
    potential_vertex = PotentialVertex(
        'IdSource', internal_id, {'id_source': id_source}, identifier_stem, id_source, 'id_source')

    vertexes = [potential_vertex]

    link_histories = kwargs.get('new_link_histories', [])
    new_links = kwargs.get('new_links', [])
    new_unlinks = kwargs.get('new_unlinks', [])
    for entry in link_histories:
        vertexes.append(entry.potential_vertex)
        edges.append(entry.generate_edge(entry.most_recent_link))
    edges.extend(x[0].generate_edge(x[1]) for x in new_links)
    edges.extend(x[0].generate_edge(x[1]) for x in new_unlinks)
    ogm = Ogm(**kwargs)
    ogm.graph_objects(vertexes, edges)


@xray_recorder.capture('get_local_ids')
@task('get_local_ids')
def get_local_ids(**kwargs):
    from toll_booth.alg_obj.graph.index_manager.index_manager import IndexManager
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = kwargs['driving_identifier_stem']
    driving_identifier_stem = IdentifierStem.from_raw(driving_identifier_stem)
    index_driver = IndexManager.from_graph_schema(kwargs['schema'], **kwargs)
    local_id_values = index_driver.get_local_id_values(driving_identifier_stem)
    return {'local_id_values': local_id_values}


@xray_recorder.capture('get_remote_ids')
@task('get_remote_ids')
def get_remote_ids(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
    from datetime import datetime

    link_utc_timestamp = datetime.utcnow()

    remote_id_extractor = _build_remote_id_extractor(**kwargs)
    with CredibleFrontEndDriver(remote_id_extractor['id_source']) as driver:
        remote_ids = driver.get_monitor_extraction(**remote_id_extractor)
        results = set([Decimal(x) for x in remote_ids])
        return {'remote_id_values': results, 'link_utc_timestamp': link_utc_timestamp}


@xray_recorder.capture('work_remote_id_change_type')
@task('work_remote_id_change_type')
def work_remote_id_change_type(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    identifier_stem = IdentifierStem.from_raw(kwargs['identifier_stem'])
    changelog_types = kwargs['changelog_types']
    change_category = changelog_types.categories[kwargs['category_id']]
    with CredibleFrontEndDriver(driving_identifier_stem.get('id_source')) as driver:
        extraction_args = {
            'driving_id_type': driving_identifier_stem.get('id_type'),
            'driving_id_name': driving_identifier_stem.get('id_name'),
            'driving_id_value': kwargs['id_value'],
            'local_max_value':  kwargs['local_max_value'],
            'category_id': change_category.category_id,
            'driving_identifier_stem': driving_identifier_stem,
            'identifier_stem': identifier_stem,
            'category': change_category
        }
        remote_changes = driver.get_change_logs(**extraction_args)
        sorted_changes = {}
        for remote_change in remote_changes:
            change_action = remote_change['Action']
            if change_action not in sorted_changes:
                sorted_changes[change_action] = []
            sorted_changes[change_action].append(remote_change)
        return {'change_actions': sorted_changes}


@xray_recorder.capture('get_enrichment_for_change_action')
@task('get_enrichment_for_change_action')
def get_enrichment_for_change_action(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe.mule_team import CredibleMuleTeam
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    id_source = driving_identifier_stem.get('id_source')
    changelog_types = kwargs['changelog_types']
    action_id = kwargs['action_id']
    change_action = changelog_types[str(action_id)]
    category_id = changelog_types.get_category_id_from_action_id(str(action_id))
    if change_action.is_static and change_action.has_details is False and not change_action.entity_type:
        empty_data = {'change_detail': {}, 'by_emp_ids': {}, 'entity_ids': {}}
        return {'enriched_data': empty_data}
    mule_team = CredibleMuleTeam(id_source)
    enrichment_args = {
        'driving_id_type': driving_identifier_stem.get('id_type'),
        'driving_id_name': driving_identifier_stem.get('id_name'),
        'driving_id_value': kwargs['id_value'],
        'local_max_value': kwargs['local_max_value'],
        'category_id': category_id,
        'action_id': int(action_id),
        'get_details': change_action.has_details is True,
        'get_by_emp_ids': change_action.is_static is False,
        'get_entity_ids': change_action.entity_type,
        'checked_emp_ids': None
    }
    enriched_data = mule_team.enrich_data(**enrichment_args)
    return {'enriched_data': enriched_data}


@xray_recorder.capture('build_mapping')
@task('build_mapping')
def build_mapping(**kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    id_source = driving_identifier_stem.get('id_source')
    schema = kwargs['schema']
    schema_entry = schema[driving_identifier_stem.object_type]
    fungal_extractor = schema_entry.extract['CredibleFrontEndExtractor']
    extraction_properties = fungal_extractor.extraction_properties
    mapping = extraction_properties['mapping']
    id_source_mapping = mapping.get(id_source, mapping['default'])
    mapping = id_source_mapping[driving_identifier_stem.get('id_type')]

    return {'mapping': mapping}


@xray_recorder.capture('batch_generate_remote_id_change_data')
@task('batch_generate_remote_id_change_data')
def batch_generate_remote_id_change_data(**kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    id_values = set()
    change_data = []
    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    remote_changes = kwargs['remote_changes']
    changelog_types = kwargs['changelog_types']
    action_id = kwargs['action_id']
    change_action = changelog_types[str(action_id)]
    enriched_data = kwargs['enriched_data']
    id_source = driving_identifier_stem.get('id_source')
    for remote_change in remote_changes:
        change_date_utc = remote_change['UTCDate']
        utc_timestamp = str(change_date_utc.timestamp())
        by_emp_id = enriched_data['by_emp_ids'].get(utc_timestamp, kwargs['id_value'])
        id_values.add((id_source, 'Employees', 'emp_id', by_emp_id))
        extracted_data = _build_change_log_extracted_data(remote_change, kwargs['mapping'])

        fungal_stem = FungalStem.from_identifier_stem(driving_identifier_stem, kwargs['id_value'], change_action.category)
        source_data = {
            'change_date_utc': extracted_data['change_date_utc'],
            'change_description': extracted_data['change_description'],
            'change_date': extracted_data['change_date'],
            'fungal_stem': str(fungal_stem),
            'action': extracted_data['action'],
            'action_id': str(action_id),
            'id_source': id_source,
            'id_type': 'ChangeLog',
            'id_name': 'change_date_utc',
            'by_emp_id': by_emp_id
        }
        returned_data = {
            'source': source_data,
            'by_emp_id_target': [{
                'id_source': id_source,
                'id_type': 'Employees',
                'id_value': by_emp_id
            }],
            'change_target': [],
            'changed_target': []
        }
        changed_targets = _build_changed_targets(id_source, extracted_data, change_action)
        if changed_targets:
            id_values.update((id_source, x['id_type'], x['id_name'], x['id_value'] for x in changed_targets))
            returned_data['changed_target'].extend(changed_targets)
        change_detail_data = enriched_data.get('change_details', {})
        change_details = enriched_data.get('change_detail', {})
        changelog_id = change_details.get(utc_timestamp)
        change_detail_data = change_detail_data.get(changelog_id)
        if change_detail_data is not None:
            for entry in change_detail_data:
                returned_data['change_target'].append({
                    'change_date_utc': extracted_data['change_date_utc'],
                    'id_source': id_source,
                    'id_type': 'Change',
                    'id_name': 'change_date_utc',
                    'changelog_id': Decimal(changelog_id),
                    'field_name': entry['id_name'],
                    'old_value': entry['old_value'],
                    'new_value': entry['new_value']
                })
        change_data.append(returned_data)
    return {'change_data': change_data, 'ext_id_values': id_values}


@xray_recorder.capture('generate_remote_id_change_data')
@task('generate_remote_id_change_data')
def generate_remote_id_change_data(**kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    remote_change = kwargs['remote_change']
    changelog_types = kwargs['changelog_types']
    action_id = kwargs['action_id']
    change_action = changelog_types[str(action_id)]
    enriched_data = kwargs['enriched_data']
    change_date_utc = remote_change['UTCDate']
    extracted_data = _build_change_log_extracted_data(remote_change, kwargs['mapping'])
    id_source = driving_identifier_stem.get('id_source')
    by_emp_id = enriched_data['emp_ids'].get(change_date_utc, kwargs['id_value'])
    fungal_stem = FungalStem.from_identifier_stem(driving_identifier_stem, kwargs['id_value'], change_action.category)
    source_data = {
        'change_date_utc': extracted_data['change_date_utc'],
        'change_description': extracted_data['change_description'],
        'change_date': extracted_data['change_date'],
        'fungal_stem': str(fungal_stem),
        'action': extracted_data['action'],
        'action_id': str(action_id),
        'id_source': id_source,
        'id_type': 'ChangeLog',
        'id_name': 'change_date_utc',
        'by_emp_id': by_emp_id
    }
    returned_data = {
        'source': source_data,
        'by_emp_id_target': [{
            'id_source': id_source,
            'id_type': 'Employees',
            'id_value': by_emp_id
        }],
        'change_target': [],
        'changed_target': []
    }
    changed_targets = _build_changed_targets(id_source, extracted_data, change_action)
    if changed_targets:
        returned_data['changed_target'].extend(changed_targets)
    change_details = enriched_data.get('change_detail', {})
    change_detail_target = change_details.get(change_date_utc, None)
    if change_detail_target is not None:
        returned_data['change_target'].extend(change_detail_target)
    return {'change_data': returned_data}


@xray_recorder.capture('post_process_get_encounters')
@task('post_process_get_encounters')
def post_process_get_encounters(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    encounter_id = kwargs['encounter_id']
    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    id_source = driving_identifier_stem.get('id_source')

    with CredibleFrontEndDriver(id_source) as driver:
        results = driver.retrieve_client_encounter(encounter_id)
        return {'encounter_results': results}


@xray_recorder.capture('post_process_parse_encounter')
@task('post_process_parse_encounters')
def post_process_parse_encounters(**kwargs):
    import bs4
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
    import dateutil

    encounter_id = kwargs['encounter_id']
    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    id_source = driving_identifier_stem.get('id_source')
    encounter_data = []
    encounter_documentation = []

    fields_of_interest = {
        'Service Type:': {
            'field_name': 'visit_type',
            'data_type': 'String'
        },
        'Program:': {
            'field_name': 'program',
            'data_type': 'String'
        },
        'Location:': {
            'field_name': 'location',
            'data_type': 'String'
        },
        'Recipient:': {
            'field_name': 'recipient',
            'data_type': 'String'
        },
        'Time In:': {
            'field_name': 'time_in',
            'data_type': 'Time'
        },
        'Time Out:': {
            'field_name': 'time_out',
            'data_type': 'Time'
        },
        'Date:': {
            'field_name': 'encounter_date',
            'data_type': 'Date'
        },
        'Duration:': {
            'field_name': 'duration',
            'data_type': 'Number'
        },
        'CPT Code:': {
            'field_name': 'cpt_code',
            'data_type': 'String'
        },
        'Non Billable:': {
            'field_name': 'non_billable',
            'data_type': 'Boolean'
        },
        'Transferred:': {
            'field_name': 'transfer_date',
            'data_type': 'DateTime'
        },
        'Approved': {
            'field_name': 'appr',
            'data_type': 'Boolean'
        }
    }
    header_data = {}
    data_fields = []
    results = kwargs['encounter_results']
    encounter_soup = bs4.BeautifulSoup(results)
    tables = encounter_soup.find_all('table')
    encounter_header_table = tables[3]
    encounter_documentation_table = tables[7]
    encounter_data.extend(_filter_string(x) for x in encounter_header_table.strings if _filter_string(x))
    encounter_documentation.extend(_filter_string(x) for x in encounter_documentation_table.strings if _filter_string(x))

    for entry in encounter_data:
        if entry in fields_of_interest:
            entry_index = encounter_data.index(entry)
            entry_index += 1
            next_field = encounter_data[entry_index]
            field_name = fields_of_interest[entry]['field_name']
            data_type = fields_of_interest[entry]['data_type']
            header_data[field_name] = next_field
            data_fields.append({
                'field_name': field_name,
                'field_value': next_field,
                'field_data_type': data_type,
                'source_id_value': encounter_id,
                'source_id_type': 'ClientVisit',
                'source_id_source': id_source
            })

    datetime_in = dateutil.parser.parse(f"{header_data['encounter_date']} {header_data['time_in']}")
    data_fields.append({
        'field_name': 'datetime_in',
        'field_value': datetime_in.isoformat(),
        'field_data_type': 'DateTime',
        'source_id_value': encounter_id,
        'source_id_type': 'ClientVisit',
        'source_id_source': id_source
    })
    datetime_out = dateutil.parser.parse(f"{header_data['encounter_date']} {header_data['time_out']}")
    data_fields.append({
        'field_name': 'datetime_out',
        'field_value': datetime_out.isoformat(),
        'field_data_type': 'DateTime',
        'source_id_value': encounter_id,
        'source_id_type': 'ClientVisit',
        'source_id_source': id_source
    })
    return {'data_fields': data_fields}


def _filter_string(question_string):
    test_string = str(question_string)
    empty_characters = ['\n', '\r']
    for entry in empty_characters:
        test_string = test_string.replace(entry, '')
    test_string = test_string.strip()
    if test_string:
        return test_string
    return False


def _build_change_log_extracted_data(remote_change, mapping):
    from toll_booth.alg_tasks.rivers.tasks.fungi import fungi_mutations

    extracted_data = {}
    for field_name, field_value in remote_change.items():
        if field_name in mapping:
            row_mapping = mapping[field_name]
            field_name = row_mapping['name']
            mutation = row_mapping['mutation']
            if mutation and field_value:
                field_value = getattr(fungi_mutations, mutation)(field_value)
            extracted_data[field_name] = field_value
    return extracted_data


def _build_changed_targets(id_source, extracted_data, change_type):
    from decimal import Decimal

    changed_target = []
    client_id = extracted_data.get('client_id', None)
    clientvisit_id = extracted_data.get('clientvisit_id', None)
    change_date_utc = extracted_data['change_date_utc']
    if client_id:
        changed_target.append({
            'id_source': id_source,
            'id_type': 'Clients',
            'id_name': 'client_id',
            'id_value': Decimal(client_id),
            'change_date_utc': change_date_utc
        })
    if clientvisit_id:
        if client_id != clientvisit_id:
            logging.warning(f'while running an extraction, got a row with the same client_id and clientvisit_id, this could be correct, or it could be due to a credible error, '
                            f'we assume it is due to the credible error, but if things go strange, check this out: {extracted_data}')
        else:
            changed_target.append({
                'id_source': id_source,
                'id_type': 'ClientVisit',
                'id_name': 'clientvisit_id',
                'id_value': Decimal(clientvisit_id),
                'change_date_utc': change_date_utc
            })
    if extracted_data.get('record', None):
        record = extracted_data.get('record')
        id_type = record['record_type']
        if id_type is None:
            id_type = change_type.id_type
        if id_type not in ['ClientVisit', 'Clients', 'unspecified']:
            id_name = change_type.id_name
            record_id = record['record_id']
            try:
                record_id = Decimal(record_id)
            except InvalidOperation:
                logging.warning(f'could not convert extracted data into a change_target, this may be by design, but still worth pointing out: {id_source}, {change_type}, {extracted_data}')
                return None
            changed_target.append({
                'id_source': id_source,
                'id_type': id_type,
                'id_name': id_name,
                'id_value': record_id,
                'change_date_utc': change_date_utc
            })
        if id_type == 'unspecified':
            raise RuntimeError('could not determine the id_type and id_name for: %s' % extracted_data)
    return changed_target


def _calculate_change_log_identifier_stem(extracted_data):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    pairs = {
        'id_source': extracted_data['source']['id_source'],
        'id_type': extracted_data['source']['id_type'],
        'id_name': extracted_data['source']['id_name']
    }
    identifier_stem = IdentifierStem('vertex', 'ChangeLog', pairs)
    return identifier_stem


def _build_remote_id_extractor(**kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = kwargs['driving_identifier_stem']
    schema = kwargs['schema']
    driving_identifier_stem = IdentifierStem.from_raw(driving_identifier_stem)
    schema_entry = schema[driving_identifier_stem.object_type]
    extractor_setup = {
        'id_type': driving_identifier_stem.get('id_type'),
        'type': 'CredibleFrontEndExtractor'
    }
    extractor_setup.update(driving_identifier_stem.for_extractor)
    extractor_setup.update(schema_entry.extract[extractor_setup['type']].extraction_properties)
    return extractor_setup


def _generate_link_data(**kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
    from toll_booth.alg_obj.graph.ogm.regulators import VertexRegulator
    from toll_booth.alg_obj.graph.index_manager.index_manager import IndexManager

    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    driving_vertex_regulator = VertexRegulator.get_for_object_type(driving_identifier_stem.object_type)

    id_value = kwargs.get('id_value', None)
    id_values = kwargs.get('id_values', None)
    if id_value is None and id_values is None:
        raise RuntimeError(f'tried to build link data, but both id_value and id_values are None, kwargs: {kwargs}')
    index_manager = IndexManager.from_graph_schema(kwargs['schema'])
    if id_value:
        object_data = driving_identifier_stem.for_extractor
        object_data['id_value'] = id_value
        potential_vertex = driving_vertex_regulator.create_potential_vertex(object_data)
        return {
            'potential_vertex': potential_vertex,
            'link_utc_timestamp': kwargs['link_utc_timestamp']
        }, index_manager
    vertexes = []
    for id_value in id_values:
        object_data = driving_identifier_stem.for_extractor
        object_data['id_value'] = id_value
        potential_vertex = driving_vertex_regulator.create_potential_vertex(object_data)
        vertexes.append(potential_vertex)

    return {
       'potential_vertexes': vertexes,
       'link_utc_timestamp': kwargs['link_utc_timestamp']
    }, index_manager


def _set_changed_ids(change_type, **kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
    from toll_booth.alg_obj.graph.ogm.regulators import VertexRegulator
    from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
    from botocore.exceptions import ClientError

    id_values = kwargs['id_values']
    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    driving_vertex_regulator = VertexRegulator.get_for_object_type(driving_identifier_stem.object_type)
    leech_driver = LeechDriver(table_name=kwargs.get('table_name', 'VdGraphObjects'))
    for id_value in id_values:
        object_data = driving_identifier_stem.for_extractor
        object_data['id_value'] = id_value
        potential_vertex = driving_vertex_regulator.create_potential_vertex(object_data)
        try:
            if change_type == 'new':
                leech_driver.set_assimilated_vertex(
                    potential_vertex, False, identifier_stem=driving_identifier_stem, id_value=id_value)
                continue
            if change_type == 'link':
                leech_driver.set_link_object(
                    potential_vertex.internal_id, driving_identifier_stem.get('id_source'), False,
                    identifier_stem=driving_identifier_stem, id_value=id_value
                )
                continue
            if change_type == 'unlink':
                leech_driver.set_link_object(
                    potential_vertex.internal_id, driving_identifier_stem.get('id_source'), True,
                    identifier_stem=driving_identifier_stem, id_value=id_value
                )
                continue
            raise NotImplementedError('could not find operation to perform for changed_ids type: %s' % change_type)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
