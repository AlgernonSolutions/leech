from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import ActivitySignature, chain
from toll_booth.alg_tasks.rivers.rocks import workflow


# @xray_recorder.capture('send_icfs_reports')
@workflow('send_icfs_reports')
def send_icfs_reports(**kwargs):
    decisions = kwargs['decisions']
    great_chain = _build_great_chain(**kwargs)
    results = great_chain(**kwargs)
    if results is None:
        return
    decisions.append(CompleteWork(results))


def _build_great_chain(**kwargs):
    execution_id = kwargs['execution_id']
    signatures = [
        ActivitySignature(f'get_productivity-{execution_id}', 'get_productivity_report_data', **kwargs),
        ActivitySignature(f'build_team-{execution_id}', 'build_clinical_teams', **kwargs),
        ActivitySignature(f'build_caseload-{execution_id}', 'build_clinical_caseloads', **kwargs),
        ActivitySignature(f'build_daily_report-{execution_id}', 'build_daily_report', **kwargs),
        ActivitySignature(f'write_report_data-{execution_id}', 'write_report_data', **kwargs),
        ActivitySignature(f'send_report-{execution_id}', 'send_report', **kwargs),
    ]
    return chain(*tuple(signatures))
