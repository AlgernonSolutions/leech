import logging
import os


class ClerkSwarm:
    def __init__(self, table_name, send_task_name='batch_dynamo_write', pending_entries=None, max_retries=3):
        if not pending_entries:
            pending_entries = []
        self._table_name = table_name
        self._send_task_name = send_task_name
        self._pending_entries = pending_entries
        self._keys = set()
        for pending_entry in pending_entries:
            self._keys.add((pending_entry['sid_value'], pending_entry['identifier_stem']))
        self._current_retries = 0
        self._max_retries = max_retries
        self._receipt = {}

    def batched_entries(self):
        batches = []
        entries = []
        counter = 0
        for entry in self._pending_entries:
            if len(entries) >= 25:
                batches.append({'entries': entries})
                entries = []
            counter += 1
            self._receipt[str(counter)] = entry
            entries.append({'item': {'Item': entry}, 'id': str(counter)})
        if entries:
            batches.append({'entries': entries})
        return batches

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

    def add_pending_write(self, pending_item):
        key_value = (pending_item['sid_value'], pending_item['identifier_stem'])
        if key_value in self._keys:
            raise RuntimeError('duplicate item added to clerk_swarm: %s' % pending_item)
        self._keys.add(key_value)
        self._pending_entries.append(pending_item)

    def send(self):
        from toll_booth.alg_obj.aws.matryoshkas.matryoshka import Matryoshka, MatryoshkaCluster
        batched_entries = self.batched_entries()
        self._pending_entries = []
        if not batched_entries:
            return True
        lambda_arn = os.environ['WORK_FUNCTION']
        task_params = {'table_name': self._table_name}
        logging.info('preparing to send a batch of write commands through the clerk swarm')
        m_cluster = MatryoshkaCluster.calculate_for_concurrency(
            100, self._send_task_name, lambda_arn,
            task_args=batched_entries, task_constants=task_params, max_m_concurrency=25)
        m = Matryoshka.for_root(m_cluster)
        logging.info('completed the clerk send')
        agg_results = m.aggregate_results
        failed_messages = []
        for result in agg_results:
            try:
                failed_messages.extend(result['failed'])
            except TypeError:
                pass
        if failed_messages:
            logging.info('some writes failed, going to retry')
            for failed_message in failed_messages:
                failed_id_value = failed_message['id']
                self._pending_entries.append(self._receipt[failed_id_value])
            self._current_retries += 1
            if self._current_retries > self._max_retries:
                logging.warning('reached maximum retry count, still have errors: %s' % self._pending_entries)
                return
            self.send()
