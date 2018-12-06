import logging
import os


class ClerkSwarm:
    def __init__(self, table_name, pending_entries=None, max_retries=3):
        if not pending_entries:
            pending_entries = []
        self._table_name = table_name
        self._pending_entries = pending_entries
        self._max_retries = max_retries

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            import traceback
            logging.warning('exception while running the swarm send: %s, %s' % (exc_type, exc_val))
            traceback.print_exc()
        if self._pending_entries:
            self.send()
        return True

    def send(self):
        from toll_booth.alg_obj.aws.matryoshkas.matryoshka import Matryoshka, MatryoshkaCluster
        receipt = {}
        send_task_name = 'batch_dynamo_write'
        lambda_arn = os.environ['WORK_FUNCTION']
        task_params = {'table_name': self._table_name}
        logging.info('preparing to send a batch of write commands through the clerk swarm')
        m_cluster = MatryoshkaCluster.calculate_for_concurrency(
            100, send_task_name, lambda_arn,
            task_args=self._pending_entries, task_constants=task_params, max_m_concurrency=25)
        m = Matryoshka.for_root(m_cluster)
        logging.info('completed the clerk send')
        agg_results = m.aggregate_results
        failed_messages = []
        for result in agg_results:
            print(result)
        pass
