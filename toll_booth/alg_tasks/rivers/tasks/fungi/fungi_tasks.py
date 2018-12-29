def get_local_max_change_type_value(**kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
    from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver, EmptyIndexException

    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    driving_id_value = kwargs['driving_id_value']
    id_source = driving_identifier_stem.get('id_source')
    id_type = driving_identifier_stem.get('id_type')
    change_category = kwargs['category']
    change_stem = f'#{id_source}#{id_type}#{driving_id_value}#{change_category}'
    leech_driver = LeechDriver(table_name=kwargs.get('table_name', 'VdGraphObjects'))
    try:
        local_max_value = leech_driver.scan_index_value_max(change_stem)
    except EmptyIndexException:
        local_max_value = None
    return local_max_value


def pull_change_types(**kwargs):
    from toll_booth.alg_obj.forge.credible_specifics import ChangeTypes
    from toll_booth.alg_obj.aws.snakes.snakes import StoredData

    change_types = ChangeTypes.get(**kwargs)
    stored_data = StoredData.from_object('change_types', change_types, full_unpack=True)
    return stored_data


def unlink_old_ids(**kwargs):
    _set_changed_ids(change_type='unlink', **kwargs)


def link_new_ids(**kwargs):
    _set_changed_ids(change_type='link', **kwargs)


def put_new_ids(**kwargs):
    _set_changed_ids(change_type='new', **kwargs)


def get_local_ids(**kwargs):
    from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
    from toll_booth.alg_obj.graph.ogm.regulators import VertexRegulator
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = kwargs['driving_identifier_stem']
    driving_identifier_stem = IdentifierStem.from_raw(driving_identifier_stem)
    driving_vertex_regulator = VertexRegulator.get_for_object_type(driving_identifier_stem.object_type)
    leech_driver = LeechDriver(table_name=kwargs.get('table_name', 'VdGraphObjects'))
    local_id_values = leech_driver.get_local_id_values(driving_identifier_stem, vertex_regulator=driving_vertex_regulator)
    return local_id_values


def get_remote_ids(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver

    remote_id_extractor = _build_remote_id_extractor(**kwargs)
    with CredibleFrontEndDriver(remote_id_extractor['id_source']) as driver:
        remote_ids = driver.get_monitor_extraction(**remote_id_extractor)
        results = set(remote_ids)
        return results


def work_remote_id_change_type(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
    from toll_booth.alg_obj.aws.snakes.snakes import StoredData

    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    identifier_stem = IdentifierStem.from_raw(kwargs['identifier_stem'])
    id_value = kwargs['id_value']
    local_max_value = kwargs['local_max_value']
    change_category = kwargs['category']
    with CredibleFrontEndDriver(driving_identifier_stem.get('id_source')) as driver:
        extraction_args = {
            'driving_id_type': driving_identifier_stem.get('id_type'),
            'driving_id_name': driving_identifier_stem.get('id_name'),
            'driving_id_value': id_value,
            'local_max_value': local_max_value,
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
        stored_data = StoredData.from_object('change_actions', sorted_changes, full_unpack=True)
        return stored_data


def get_enrichment_for_change_action(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe.mule_team import CredibleMuleTeam
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    id_value = kwargs['id_value']
    id_source = driving_identifier_stem.get('id_source')
    change_category = kwargs['change_category']
    action_id = kwargs['action_id']
    local_max_value = kwargs['local_max_value']
    change_action = change_category[action_id]
    mule_team = CredibleMuleTeam(id_source)
    enrichment_args = {
        'driving_id_type': driving_identifier_stem.get('id_type'),
        'driving_id_name': driving_identifier_stem.get('id_name'),
        'driving_id_value': id_value,
        'local_max_value': local_max_value,
        'category_id': change_category.category_id,
        'action_id': int(change_action.action_id),
        'get_details': change_action.has_details is True,
        'get_emp_ids': change_action.is_static is False,
        'checked_emp_ids': None
    }
    enriched_data = mule_team.enrich_data(**enrichment_args)
    return enriched_data


def generate_remote_id_change_data(**kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = IdentifierStem.from_raw(kwargs['driving_identifier_stem'])
    remote_change = kwargs['remote_change']
    change_category = kwargs['change_category']
    change_type = kwargs['change_type']
    enriched_data = kwargs['enriched_data']
    change_date_utc = remote_change['UTCDate']
    extracted_data = _build_change_log_extracted_data(remote_change, self._mapping)
    id_source = driving_identifier_stem.get('id_source')
    source_data = {
        'change_date_utc': extracted_data['change_date_utc'],
        'change_description': extracted_data['change_description'],
        'change_date': extracted_data['change_date'],
        'action': extracted_data['action'],
        'action_id': change_category.get_action_id(extracted_data['action']),
        'id_source': id_source,
        'id_type': 'ChangeLog',
        'id_name': 'change_date_utc',
        'by_emp_id': enriched_data['emp_ids'].get(change_date_utc, kwargs['id_value'])
    }
    returned_data = {
        'source': source_data
    }
    changed_target = _build_changed_targets(id_source, extracted_data, change_type)
    if changed_target:
        returned_data['changed_target'] = changed_target
    change_target = enriched_data['change_detail'].get(change_date_utc, None)
    if change_target is not None:
        returned_data['change_target'] = change_target
    return returned_data


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
    if client_id and client_id != 0:
        changed_target.append({
            'id_source': id_source,
            'id_type': 'Clients',
            'id_name': 'client_id',
            'id_value': Decimal(client_id)
        })
    if clientvisit_id and clientvisit_id != '0':
        changed_target.append({
            'id_source': id_source,
            'id_type': 'ClientVisit',
            'id_name': 'clientvisit_id',
            'id_value': Decimal(clientvisit_id)
        })
    if extracted_data.get('record', None):
        record = extracted_data.get('record')
        id_type = record['record_type']
        if id_type is None:
            id_type = change_type.id_type
        if id_type not in ['ClientVisit', 'Clients', 'unspecified']:
            id_name = change_type.id_name
            changed_target.append({
                'id_source': id_source,
                'id_type': id_type,
                'id_name': id_name,
                'id_value': Decimal(record['record_id'])
            })
        if id_type == 'unspecified':
            static_values = change_type.change_target
            if static_values == 'dynamic':
                raise RuntimeError('could not determine the id_type and id_name for: %s' % extracted_data)
            static_values['id_value'] = Decimal(record['record_id'])
            static_values['id_source'] = id_source
            changed_target.append(static_values)
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
    from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry
    from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = kwargs['driving_identifier_stem']
    driving_identifier_stem = IdentifierStem.from_raw(driving_identifier_stem)
    schema_entry = SchemaVertexEntry.get(driving_identifier_stem.object_type)
    leech_driver = LeechDriver(table_name=kwargs.get('table_name', 'VdGraphObjects'))
    extractor_setup = leech_driver.get_extractor_setup(driving_identifier_stem)
    extractor_setup.update(driving_identifier_stem.for_extractor)
    extractor_setup.update(schema_entry.extract[extractor_setup['type']].extraction_properties)
    return extractor_setup


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
