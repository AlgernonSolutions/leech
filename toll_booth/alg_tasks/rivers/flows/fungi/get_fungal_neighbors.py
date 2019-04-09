from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
from toll_booth.alg_tasks.rivers.rocks import workflow


@xray_recorder.capture('get_fungal_neighbors')
@workflow('get_fungal_neighbors')
def get_fungal_neighbors(**kwargs):

    execution_id = kwargs['execution_id']
    names = {
        'local': f'get_local_ids-{execution_id}',
        'remote': f'get_remote_ids-{execution_id}',
        'work_links': f'work_fip_links-{execution_id}',
        'change_types': f'pull_change_types-{execution_id}',
        'schema_entry': f'pull_schema_entry-{execution_id}',
        'map': f'build_mapping-{execution_id}'
    }
    kwargs['names'] = names
    decisions = kwargs['decisions']
    great_chain = _build_chain(**kwargs)
    chain_results = great_chain(**kwargs)
    if chain_results is None:
        return
    great_group = _build_group(**kwargs)
    group_results = great_group(**kwargs)
    if group_results is None:
        return
    decisions.append(CompleteWork())


def _check_recursion_depth(task_args, **kwargs):
    branches = task_args.get_argument_value('recursion_branches')
    identifier_stem_value = task_args.get_argument_value('identifier_stem')
    identifier_stem = IdentifierStem.from_raw(identifier_stem_value)
    try:
        next_identifier_stem = branches[branches.index(identifier_stem)+1]
        return identifier_stem, next_identifier_stem
    except IndexError:
        return False
