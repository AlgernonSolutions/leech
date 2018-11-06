import json
import os

import boto3

from toll_booth.alg_obj.aws.matryoshkas.bees import OrderSwarm
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder


class SmallSwarm:
    def __init__(self, queue_url):
        self._messages = []
        self._queue = boto3.resource('sqs').Queue(queue_url)

    def add_order(self, order):
        self._messages.append(order)

    def add_orders(self, orders):
        for order in orders:
            self._messages.append(order)

    def send(self):
        counter = 1
        entries = []
        for order in self._messages:
            if len(entries) >= 10:
                self._queue.send_messages(
                    Entries=entries
                )
                entries = []
                counter = 1
            entries.append({
                'Id': str(counter),
                'MessageBody': json.dumps(order, cls=AlgEncoder)
            })
            counter += 1
        if entries:
            self._queue.send_messages(
                Entries=entries
            )


class ForgeQueue:
    def __init__(self, queue_name, queue_url, swarm=True):
        order_swarm = SmallSwarm(queue_url)
        self._queue_name = queue_name
        self._queue_url = queue_url
        if swarm:
            order_swarm = OrderSwarm(queue_url)
        self._order_swarm = order_swarm

    @classmethod
    def get_for_extraction_queue(cls, **kwargs):
        default_queue_url = 'https://sqs.us-east-1.amazonaws.com/803040539655/extraction'
        queue_url = kwargs.get('queue_url', os.getenv('EXTRACTION_URL', default_queue_url))
        return cls('extraction_queue', queue_url)

    @classmethod
    def get_for_transform_queue(cls, **kwargs):
        default_queue_url = 'https://sqs.us-east-1.amazonaws.com/803040539655/transform'
        queue_url = kwargs.get('queue_url', os.getenv('TRANSFORM_URL', default_queue_url))
        return cls('transform_queue', queue_url, swarm=False)

    @classmethod
    def get_for_assimilation_queue(cls, **kwargs):
        default_queue_url = 'https://sqs.us-east-1.amazonaws.com/803040539655/assimilate'
        queue_url = kwargs.get('queue_url', os.getenv('ASSIMILATE_URL', default_queue_url))
        return cls('assimilate_queue', queue_url)

    @classmethod
    def get_for_load_queue(cls, **kwargs):
        default_queue_url = 'https://sqs.us-east-1.amazonaws.com/803040539655/load'
        queue_url = kwargs.get('queue_url', os.getenv('LOAD_URL', default_queue_url))
        return cls('load_queue', queue_url)

    @classmethod
    def get_for_process_queue(cls, **kwargs):
        default_queue_url = 'https://sqs.us-east-1.amazonaws.com/803040539655/process'
        queue_url = kwargs.get('queue_url', os.getenv('PROCESS_URL', default_queue_url))
        return cls('process_queue', queue_url)

    def add_order(self, order):
        self._order_swarm.add_order(order.to_work)

    def add_orders(self, orders):
        for order in orders:
            self._order_swarm.add_order(order.to_work)

    def push_orders(self):
        return self._order_swarm.send()

    def get_orders(self, num_orders=1):
        orders = []
        sqs = boto3.resource('sqs')
        queue = sqs.Queue(self._queue_url)
        messages = queue.receive_messages(
            MaxNumberOfMessages=num_orders
        )
        for message in messages:
            test_string = json.loads(message.body)
            transform_order = json.loads(test_string, cls=AlgDecoder)
            orders.append(transform_order)
            message.delete()
        return orders

    def __len__(self):
        return len(self._order_swarm.outbound_orders)
