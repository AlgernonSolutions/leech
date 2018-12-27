import json

from toll_booth.alg_obj.serializers import AlgEncoder
from toll_booth.alg_tasks.rivers.fungi_tasks import get_ids, manage_ids, set_changed_ids, build_remote_id_extractor, \
    work_remote_ids, get_change_types
from toll_booth.alg_tasks.rivers.rocks import workflow


@workflow
def command_fungi(markers, **kwargs):
    execution_id = kwargs['execution_id']
    kwargs['names'] = {
        'local': f'get_local_ids-{execution_id}',
        'remote': f'get_remote_ids-{execution_id}',
        'put': f'put_new_ids-{execution_id}',
        'link': f'link_new_ids-{execution_id}',
        'unlink': f'unlink_old_ids-{execution_id}',
        'change_types': f'pull_change_types-{execution_id}'
    }
    if 'get_ids' not in markers:
        return get_ids(**kwargs)
    if 'manage_ids' not in markers:
        return manage_ids(**kwargs)
    if 'change_types' not in markers:
        return get_change_types(**kwargs)
    work_remote_ids(**kwargs)


@workflow
def get_remote_ids(input_kwargs, **kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
    from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork

    remote_id_extractor = build_remote_id_extractor(input_kwargs['driving_identifier_stem'], **kwargs)
    with CredibleFrontEndDriver(remote_id_extractor['id_source']) as driver:
        remote_ids = driver.get_monitor_extraction(**remote_id_extractor)
        results = json.dumps(set(remote_ids), cls=AlgEncoder)
        kwargs['decisions'].append(CompleteWork(results))


@workflow
def get_local_ids(input_kwargs, **kwargs):
    from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
    from toll_booth.alg_obj.graph.ogm.regulators import VertexRegulator
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
    from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork

    driving_identifier_stem = input_kwargs['driving_identifier_stem']
    driving_identifier_stem = IdentifierStem.from_raw(driving_identifier_stem)
    driving_vertex_regulator = VertexRegulator.get_for_object_type(driving_identifier_stem.object_type)
    leech_driver = LeechDriver(table_name=kwargs.get('table_name', 'VdGraphObjects'))
    local_id_values = leech_driver.get_local_id_values(driving_identifier_stem, vertex_regulator=driving_vertex_regulator)
    results = json.dumps(local_id_values, cls=AlgEncoder)
    kwargs['decisions'].append(CompleteWork(results))
    return


@workflow
def put_new_ids(**kwargs):
    from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork

    set_changed_ids(change_type='new', **kwargs)
    kwargs['decisions'].append(CompleteWork())


@workflow
def link_new_ids(**kwargs):
    from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork

    set_changed_ids(change_type='link', **kwargs)
    kwargs['decisions'].append(CompleteWork())


@workflow
def unlink_old_ids(**kwargs):
    from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork

    set_changed_ids(change_type='unlink', **kwargs)
    kwargs['decisions'].append(CompleteWork())


@workflow
def pull_change_types(**kwargs):
    from toll_booth.alg_obj.forge.credible_specifics import ChangeTypes
    from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork

    change_types = ChangeTypes.get(**kwargs)
    results = json.dumps(change_types, cls=AlgEncoder)
    kwargs['decisions'].append(CompleteWork(results))


@workflow
def get_local_max_change_type_value(input_kwargs, **kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
    from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver, EmptyIndexException
    from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork

    driving_identifier_stem = IdentifierStem.from_raw(input_kwargs['driving_identifier_stem'])
    driving_id_value = input_kwargs['driving_id_value']
    id_source = driving_identifier_stem.get('id_source')
    id_type = driving_identifier_stem.get('id_type')
    change_category = input_kwargs['category']
    change_stem = f'#{id_source}#{id_type}#{driving_id_value}#{change_category}'
    leech_driver = LeechDriver(table_name=kwargs.get('table_name', 'VdGraphObjects'))
    try:
        local_max_value = leech_driver.scan_index_value_max(change_stem)
    except EmptyIndexException:
        local_max_value = None
    kwargs['decisions'].append(CompleteWork(local_max_value))


@workflow
def work_remote_id_change_type(input_kwargs, **kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    driving_identifier_stem = IdentifierStem.from_raw(input_kwargs['driving_identifier_stem'])
    identifier_stem = IdentifierStem.from_raw(input_kwargs['identifier_stem'])
    with CredibleFrontEndDriver(driving_identifier_stem.get('id_source')) as driver:
        pass


@workflow
def get_remote_id_by_emp_ids(**kwargs):
    pass


@workflow
def get_remote_id_change_details(**kwargs):
    pass


@workflow
def generate_remote_id_change_data(**kwargs):
    pass


@workflow
def put_remote_id_change_data(**kwargs):
    pass
