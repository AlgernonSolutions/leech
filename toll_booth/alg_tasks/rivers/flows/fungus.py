from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import Signature
from toll_booth.alg_tasks.rivers.rocks import workflow


@workflow('fungi')
def fungus(execution_id, **kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    subtask_name = 'command_fungi'
    decisions = kwargs['decisions']
    subtask_identifier = f'f-{execution_id}'
    task_args = kwargs['task_args']
    identifier_stem = IdentifierStem.from_raw("#vertex#ChangeLog#{\"id_source\": \"MBI\"}#")
    driving_identifier_stem = IdentifierStem.from_raw("#vertex#ExternalId#{\"id_source\": \"MBI\", \"id_type\": \"Employees\", \"id_name\": \"emp_id\"}#")
    task_args.add_argument_value(subtask_name, {'identifier_stem': identifier_stem, 'driving_identifier_stem': driving_identifier_stem})
    fungal_signature = Signature.for_subtask(subtask_identifier, subtask_name, **kwargs)
    results = fungal_signature(**kwargs)
    if not results:
        return
    decisions.append(CompleteWork())
