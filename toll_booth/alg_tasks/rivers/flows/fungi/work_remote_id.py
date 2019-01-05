from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import Signature, group
from toll_booth.alg_tasks.rivers.rocks import workflow


@workflow
def work_remote_id(**kwargs):
    decisions = kwargs['decisions']
    execution_id = kwargs['execution_id']
    task_args = kwargs['task_args']
    source_kwargs = task_args['source']
    id_value = task_args['id_value']
    changelog_types = source_kwargs['changelog_types']
    signatures = []
    for category_id, change_category in changelog_types.categories.items():
        task_args.add_arguments({'source': {'category_id': category_id}})
        subtask_identifier = f'work_remote_id_change_type-{change_category}-{id_value}-{execution_id}'
        category_signature = Signature.for_subtask(subtask_identifier, 'work_remote_id_change_type', **kwargs)
        signatures.append(category_signature)
    tuple_signatures = tuple(signatures)
    great_group = group(*tuple_signatures)
    group_results = great_group(**kwargs)
    if group_results is None:
        return
    decisions.append(CompleteWork())
