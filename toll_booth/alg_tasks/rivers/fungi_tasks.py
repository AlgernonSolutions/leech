import json

from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder


def build_remote_id_extractor(driving_identifier_stem, **kwargs):
    from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry
    from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = IdentifierStem.from_raw(driving_identifier_stem)
    schema_entry = SchemaVertexEntry.get(driving_identifier_stem.object_type)
    leech_driver = LeechDriver(table_name=kwargs.get('table_name', 'VdGraphObjects'))
    extractor_setup = leech_driver.get_extractor_setup(driving_identifier_stem)
    extractor_setup.update(driving_identifier_stem.for_extractor)
    extractor_setup.update(schema_entry.extract[extractor_setup['type']].extraction_properties)
    return extractor_setup


def get_ids(execution_id, names, decisions, sub_tasks, **kwargs):
    from toll_booth.alg_obj.aws.gentlemen.decisions import StartSubtask, RecordMarker

    get_remote_id_name = names['remote']
    get_local_id_name = names['local']
    working = False
    if get_remote_id_name not in sub_tasks:
        decisions.append(StartSubtask(
            execution_id, 'get_remote_ids', kwargs.get('flow_input'), version='1',
            task_list_name='Leech', lambda_role=kwargs.get('lambda_role')
        ))
        working = True
    if get_local_id_name not in sub_tasks:
        decisions.append(StartSubtask(
            execution_id, 'get_local_ids', kwargs.get('flow_input'), version='1',
            task_list_name='Leech', lambda_role=kwargs.get('lambda_role')
        ))
        working = True
    if working:
        return
    get_remote_ids_operation = sub_tasks[get_remote_id_name]
    get_local_ids_operation = sub_tasks[get_local_id_name]
    if get_remote_ids_operation.is_complete and get_local_ids_operation.is_complete:
        decisions.append(RecordMarker('get_ids', 'completed'))
        return 'fire'


def manage_ids(execution_id, decisions, names, sub_tasks, input_kwargs, **kwargs):
    from toll_booth.alg_obj.aws.gentlemen.decisions import StartSubtask, RecordMarker

    get_local_ids_operation = sub_tasks[names['local']]
    get_remote_ids_operation = sub_tasks[names['remote']]
    put_new_ids_operation_name = names['put']
    local_id_values = json.loads(get_local_ids_operation.results, cls=AlgDecoder)
    remote_id_values = json.loads(get_remote_ids_operation.results, cls=AlgDecoder)
    local_linked_values = local_id_values['linked']
    new_id_values = remote_id_values - local_id_values['all']
    if put_new_ids_operation_name not in sub_tasks:
        input_kwargs['id_values'] = new_id_values
        flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
        decisions.append(StartSubtask(
            execution_id, 'put_new_ids', flow_input, version='1',
            task_list_name='Leech', lambda_role=kwargs.get('lambda_role')
        ))
        return
    if sub_tasks[put_new_ids_operation_name].is_complete:
        working = False
        link_new_ids_operation_name = names['link']
        unlink_old_ids_operation_name = names['unlink']
        if link_new_ids_operation_name not in sub_tasks:
            newly_linked_id_values = remote_id_values - local_linked_values
            input_kwargs['id_values'] = newly_linked_id_values
            flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
            decisions.append(StartSubtask(
                execution_id, 'link_new_ids', flow_input, version='1',
                task_list_name='Leech', lambda_role=kwargs.get('lambda_role')
            ))
            working = True
        if unlink_old_ids_operation_name not in sub_tasks:
            unlinked_id_values = local_linked_values - remote_id_values
            input_kwargs['id_values'] = unlinked_id_values
            flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
            decisions.append(StartSubtask(
                execution_id, 'unlink_old_ids', flow_input, version='1',
                task_list_name='Leech', lambda_role=kwargs.get('lambda_role')
            ))
            working = True
        if working:
            return
        if sub_tasks[link_new_ids_operation_name].is_complete and sub_tasks[unlink_old_ids_operation_name].is_complete:
            decisions.append(RecordMarker('manage_ids', 'completed'))
            return 'fire'


def set_changed_ids(input_kwargs, change_type, **kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
    from toll_booth.alg_obj.graph.ogm.regulators import VertexRegulator
    from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
    from botocore.exceptions import ClientError

    id_values = input_kwargs['id_values']
    driving_identifier_stem = IdentifierStem.from_raw(input_kwargs['driving_identifier_stem'])
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


def get_change_types(execution_id, flow_input, names, decisions, sub_tasks, **kwargs):
    from toll_booth.alg_obj.aws.gentlemen.decisions import StartSubtask, RecordMarker

    get_change_types_name = names['change_types']
    if get_change_types_name not in sub_tasks:
        decisions.append(StartSubtask(
            execution_id, 'pull_change_types', flow_input, version='1',
            task_list_name='Leech', lambda_role=kwargs.get('lambda_role')
        ))
        return
    if sub_tasks[get_change_types_name].is_completed:
        decisions.append(RecordMarker('change_types', 'completed'))
        return 'fire'


def work_remote_ids(execution_id, input_kwargs, sub_tasks, names, decisions, **kwargs):
    from toll_booth.alg_obj.aws.gentlemen.decisions import StartSubtask, RecordMarker

    get_remote_ids_operation = sub_tasks[names['remote']]
    get_change_type_operation = sub_tasks[names['change_types']]
    change_types = json.loads(get_change_type_operation.results, cls=AlgDecoder)
    remote_id_values = json.loads(get_remote_ids_operation.results, cls=AlgDecoder)
    for remote_id_value in remote_id_values:
        for category_id, change_category in change_types.categories.items():
            input_kwargs['driving_id_value'] = remote_id_value
            input_kwargs['category_id'] = str(category_id)
            input_kwargs['category'] = str(change_category)
            local_max_name = f'get_local_max_change_type_value-{change_category}-{remote_id_value}-{execution_id}'
            local_max_name_base = f'get_local_max_change_type_value-{change_category}-{remote_id_value}'
            if local_max_name not in sub_tasks:
                flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
                decisions.append(StartSubtask(
                    execution_id, 'get_local_max_change_type_value', flow_input, version='1',
                    task_list_name='Leech', lambda_role=kwargs.get('lambda_role'), name_base=local_max_name_base
                ))
                return
            local_max_operation = sub_tasks[local_max_name]
            if not local_max_operation.is_complete:
                return
            remote_category_name = f'work_remote_id_change_type-{change_category}-{remote_id_value}-{execution_id}'
            remote_category_name_base = f'work_remote_id_change_type-{change_category}-{remote_id_value}'
            if remote_category_name not in sub_tasks:
                input_kwargs['local_max_value'] = local_max_operation.results
                flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
                decisions.append(StartSubtask(
                    execution_id, 'work_remote_id_change_type', flow_input, version='1',
                    task_list_name='Leech', lambda_role=kwargs.get('lambda_role'), name_base=remote_category_name_base
                ))
                return
            category_operation = sub_tasks[remote_category_name]
            if not category_operation.is_complete:
                return
