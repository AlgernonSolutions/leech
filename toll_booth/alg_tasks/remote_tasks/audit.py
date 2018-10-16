import logging

from toll_booth.alg_obj.graph.auditing.scan_auditor import ScanAuditWorker, AuditResultsQueue
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def audit(*args, **kwargs):
    logging.info(f'received a audit call with args: {args}, kwargs: {kwargs}')
    task_args = kwargs['task_args']
    audit_worker = ScanAuditWorker(**task_args)
    missing_internal_ids = audit_worker.scan()
    if missing_internal_ids:
        results_queue = AuditResultsQueue(**task_args)
        results_queue.send_audit_results(missing_internal_ids)
