import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.serializers import AlgEncoder


class MessagePull(AlgObject):
    def __init__(self, messages):
        self._messages = messages

    @classmethod
    def get_from_queue(cls, num_messages=None, wait_time=None, visibility_time=None, queue_url=None):
        messages = []
        if not queue_url:
            queue_url = os.environ['QUEUE_URL']
        if not wait_time:
            wait_time = 20
        if not visibility_time:
            visibility_time = 120
        if not num_messages:
            num_messages = 5
        client = boto3.client('sqs')
        message_pull = client.receive_message(
            QueueUrl=queue_url,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility_time,
            MaxNumberOfMessages=num_messages
        )
        try:
            message_texts = message_pull['Messages']
            for message_text in message_texts:
                message_id = message_text['MessageId']
                receipt = message_text['ReceiptHandle']
                raw_body = message_text['Body']
                body = json.loads(raw_body)
                message = Message(message_id, raw_body, body, receipt, queue_url)
                if 'task_name' in body and 'task_args' in body:
                    message = TaskMessage(message_id, raw_body, body, receipt, queue_url)
                messages.append(message)
        except KeyError:
            return messages
        return cls(messages)

    def __iter__(self):
        return iter(self._messages)

    def __bool__(self):
        if self._messages:
            return True
        return False


class OutboundMessage(AlgObject):
    def __init__(self, message_body):
        self._message_body = message_body

    @classmethod
    def for_task(cls, task):
        return cls(task.as_json)

    @property
    def message_body(self):
        return self._message_body


class MessageSend(AlgObject):
    def __init__(self, outbound_messages=None, auto_send=False, queue_url=None):
        if not queue_url:
            queue_url = os.environ['QUEUE_URL']
        if not outbound_messages:
            outbound_messages = []
        self._outbound_messages = outbound_messages
        self._auto_send = auto_send
        self._queue_url = queue_url

    def add_message(self, outbound_message):
        if self._auto_send:
            if len(self._outbound_messages) >= 10:
                self.send()
        self._outbound_messages.append(outbound_message)

    def send(self):
        batches = []
        entries = []
        id_counter = 0
        for message in self._outbound_messages:
            if len(entries) > 10:
                batches.append(entries)
                entries = []
            id_counter += 1
            entries.append({
                'Id': str(id_counter),
                'MessageBody': json.dumps(message.message_body)
            })
        if entries:
            batches.append(entries)
        self._outbound_messages = []
        session = boto3.session.Session()
        client = session.client('sqs', config=Config(
            connect_timeout=315, read_timeout=315, max_pool_connections=25, retries={'max_attempts': 2}))
        for entries in batches:
            client.send_message_batch(
                QueueUrl=self._queue_url,
                Entries=entries
            )
            logging.debug('sent some messages: %s %s' % (self._queue_url, entries))


class Message(AlgObject):
    def __init__(self, message_id, raw_body, message_body, message_receipt_handle, source_queue_url):
        self._message_id = message_id
        self._message_body = message_body
        self._message_receipt = message_receipt_handle
        self._queue_url = source_queue_url
        self._raw_body = raw_body

    @property
    def message_body(self):
        return self._message_body

    @property
    def raw_body(self):
        return self._raw_body

    def mark_completed(self):
        client = boto3.client('sqs')
        client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=self._message_receipt
        )


class TaskMessage(Message):
    @property
    def task_name(self):
        return self._message_body['task_name']

    @property
    def task_args(self):
        return self._message_body['task_args']


class Task(AlgObject):
    def __init__(self, task_name, task_args):
        self._task_name = task_name
        self._task_args = task_args

    @property
    def task_name(self):
        return self._task_name

    @property
    def task_args(self):
        return self._task_args

    @property
    def as_json(self):
        return {'task_name': self.task_name, 'task_args': self.task_args}


class MessageSwarm(AlgObject):
    def __init__(self, target_queue_url, auto_send_threshold=None, outbound_messages=None, max_retries=3):
        if not outbound_messages:
            outbound_messages = []
        self._queue_url = target_queue_url
        self._outbound_messages = outbound_messages
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
        if self._outbound_messages:
            self.send()
        return True

    @property
    def outbound_messages(self):
        return self._outbound_messages

    def add_message(self, outbound_message):
        if self._auto_send_threshold:
            if len(self._outbound_messages) >= self._auto_send_threshold:
                self.send()
        self._outbound_messages.append(outbound_message)

    def send(self):
        from toll_booth.alg_obj.aws.aws_obj.matryoshka import Matryoshka, MatryoshkaCluster
        batches = []
        entries = []
        counter = 0
        receipt = {}
        for message in self._outbound_messages:
            if len(entries) >= 10:
                batches.append({'entries': entries})
                entries = []
            counter += 1
            receipt[str(counter)] = message
            entries.append({
                'Id': str(counter),
                'MessageBody': json.dumps(message.message_body, cls=AlgEncoder)
            })
        if entries:
            batches.append({'entries': entries})
        self._outbound_messages = []
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
                    self.add_message(receipt[message_id])
                else:
                    print('a message failed on our error: %s' % failed_message)
            self._current_retries += 1
            if self._current_retries > self._max_retries:
                print('reached maximum retry count, still have errors: %s' % self._outbound_messages)
                return
            self.send()

    def send_simply(self):
        from toll_booth.alg_tasks.remote_tasks.send_message_batch import send_message_batch
        print('sending the messages with no bother')
        batches = []
        entries = []
        counter = 0
        receipt = {}
        print('splitting up the chunks')
        total = len(self._outbound_messages)
        for message in self._outbound_messages:
            if len(entries) >= 10:
                batches.append({'entries': entries, 'target_queue_url': self._queue_url})
                entries = []
            counter += 1
            receipt[str(counter)] = message
            entries.append({
                'Id': str(counter),
                'MessageBody': json.dumps(message.message_body)
            })
            print('%s/%s' % (counter, total))
        if entries:
            batches.append({'entries': entries, 'target_queue_url': self._queue_url})
        self._outbound_messages = []
        if not batches:
            return True
        pointer = 0
        with ThreadPoolExecutor(max_workers=750) as executor:
            results = executor.map(send_message_batch, batches)
            for result in results:
                pointer += 1
                print('%s/%s' % (pointer, total))
