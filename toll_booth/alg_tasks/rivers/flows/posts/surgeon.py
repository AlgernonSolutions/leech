from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import LambdaSignature, ActivitySignature, group, chain
from toll_booth.alg_obj.aws.gentlemen.tasks import TaskArguments
from toll_booth.alg_tasks.rivers.rocks import workflow

_team_data = {
    'ICFS': {
        'teams': {
            'Bacchus': [],
            'Tali': [],
            'Bragg': [],
            'Pinkney': [],
            'Moore': [],
            'Shpak': [],
            'Pittinger-Dunham': [],
            'Other': []
        },
        'manual_assignments': {
            7335: 'Pittinger-Dunham',
            12791: 'Pittinger-Dunham',
            11655: 'Pittinger-Dunham',
            13304: 'Pittinger-Dunham',
            12869: 'Pittinger-Dunham',
            13553: 'Other'
        },
        'first_level': ['Bacchus', 'Tali', 'Bragg', 'Pinkney', 'Moore'],
        'default_team': 'Shpak'
    }
}


# @xray_recorder.capture('send_icfs_reports')
@workflow('send_icfs_reports')
def send_icfs_reports(**kwargs):
    execution_id = kwargs['execution_id']
    decisions = kwargs['decisions']
    kwargs['names'] = {
        'get_productivity': f'get_productivity-{execution_id}',
        'get_da_tx': f'get_encounters-{execution_id}'
    }
    query_group = _build_query_group(**kwargs)
    query_data = query_group(**kwargs)
    if query_data is None:
        return
    build_send_chain = _build_send_reports(**kwargs)
    final_results = build_send_chain(**kwargs)
    if final_results is None:
        return
    decisions.append(CompleteWork(final_results))


def _build_query_group(**kwargs):
    names = kwargs['names']
    productivity = ActivitySignature(names['get_productivity'], 'get_productivity_report_data', **kwargs)
    da_tx = ActivitySignature(names['get_da_tx'], 'get_da_tx_data', **kwargs)
    return group(productivity, da_tx)


def _build_send_reports(task_args: TaskArguments, **kwargs):
    execution_id = kwargs['execution_id']
    id_source = task_args.get_argument_value('id_source')
    new_task_args = task_args.replace_argument_value('collect', _team_data[id_source], execution_id)
    signatures = [
        ActivitySignature(f'build_team-{execution_id}', 'build_clinical_teams', task_args=new_task_args, **kwargs),
        ActivitySignature(f'build_productivity-{execution_id}', 'build_productivity_report', task_args=task_args, **kwargs),
        ActivitySignature(f'write_report_data-{execution_id}', 'write_report_data', task_args=task_args, **kwargs),
    ]
    return chain(*tuple(signatures))
