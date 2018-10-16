import json
import os

from toll_booth.alg_obj.serializers import AlgEncoder


class OrderSwarm:
    def __init__(self, target_queue_url, auto_send_threshold=None, outbound_orders=None, max_retries=3):
        if not outbound_orders:
            outbound_orders = []
        self._queue_url = target_queue_url
        self._outbound_orders = outbound_orders
        self._auto_send_threshold = auto_send_threshold
        self._max_retries = max_retries
        self._current_retries = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            import traceback
            print('exception while running the swarm send: %s, %s' % (exc_type, exc_val))
            traceback.print_exc()
        if self._outbound_orders:
            self.send()
        return True

    @property
    def outbound_orders(self):
        return self._outbound_orders

    def add_order(self, order):
        if self._auto_send_threshold:
            if len(self._outbound_orders) >= self._auto_send_threshold:
                self.send()
        self._outbound_orders.append(order)

    def send(self):
        from toll_booth.alg_obj.aws.aws_obj.matryoshka import Matryoshka, MatryoshkaCluster
        batches = []
        entries = []
        counter = 0
        receipt = {}
        for message in self._outbound_orders:
            if len(entries) >= 10:
                batches.append({'entries': entries})
                entries = []
            counter += 1
            receipt[str(counter)] = message
            entries.append({
                'Id': str(counter),
                'MessageBody': json.dumps(message, cls=AlgEncoder)
            })
        if entries:
            batches.append({'entries': entries})
        self._outbound_orders = []
        if not batches:
            return True
        send_task_name = 'send_message_batch'
        lambda_arn = os.environ['WORK_FUNCTION']
        task_params = {'target_queue_url': self._queue_url}
        print('preparing to send a batch of messages through the swarm')
        m_cluster = MatryoshkaCluster.calculate_for_concurrency(
            100, send_task_name, lambda_arn, task_args=batches, task_constants=task_params, max_m_concurrency=25)
        m = Matryoshka.for_root(m_cluster)
        print('completed the swarm send')
        agg_results = m.aggregate_results
        failed_messages = []
        for result in agg_results:
            try:
                failed_messages.extend(result['failed'])
            except TypeError:
                pass
        if failed_messages:
            print('some messages failed, going to retry')
            for failed_message in failed_messages:
                message_id = failed_message['Id']
                our_fault = failed_message['SenderFault']
                if not our_fault:
                    self.add_order(receipt[message_id])
                else:
                    print('a message failed on our error: %s' % failed_message)
            self._current_retries += 1
            if self._current_retries > self._max_retries:
                print('reached maximum retry count, still have errors: %s' % self._outbound_orders)
                return
            self.send()
