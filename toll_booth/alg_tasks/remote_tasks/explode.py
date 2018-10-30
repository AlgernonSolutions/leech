import json
import logging
import os
import threading
from queue import Queue

from toll_booth.alg_tasks.task_obj import remote_task


class MailBoxes:
    def __init__(self):
        destinations = {
            'graphing': self._send_for_graphing,
            'processing': self._send_for_processing
        }
        queues = {}
        threads = []
        for destination_name, destination_function in destinations.items():
            queues[destination_name] = Queue()
            t = threading.Thread(target=destination_function)
            threads.append(t)
        self._queues = queues
        self._threads = threads
        for thread in threads:
            thread.start()

    def send_mail(self, destination, sent_object):
        try:
            self._queues[destination].put(sent_object)
        except KeyError:
            raise RuntimeError(f'there is no mailbox registered for destination: {destination}')

    def get_mail(self, destination):
        try:
            return self._queues[destination].get()
        except KeyError:
            raise RuntimeError(f'there is no mailbox registered for destination: {destination}')

    def close(self):
        for queue in self._queues.values():
            queue.put(None)
        for thread in self._threads:
            thread.join()

    def _send_for_graphing(self):
        import boto3

        default_queue_url = 'https://sqs.us-east-1.amazonaws.com/803040539655/load'
        queue_url = os.getenv('LOAD_URL', default_queue_url)
        queue = boto3.resource('sqs').Queue(queue_url)
        graph_orders = []
        counter = 1
        while True:
            object_fields = self.get_mail('graphing')
            if object_fields is None:
                if graph_orders:
                    return queue.send_messages(
                        Entries=graph_orders
                    )
                return
            graph_orders.append({
                'Id': str(counter),
                'MessageBody': json.dumps({
                    'task_name': 'load',
                    'task_args': {
                        'keys': object_fields
                    }
                })
            })
            counter += 1

    def _send_for_processing(self):
        raise NotImplementedError()


@remote_task
def explode(*args, **kwargs):
    logging.info(f'started explode task with argsL {args}, kwargs: {kwargs}')
    orders = MailBoxes()
    task_args = kwargs['task_args']
    for record in task_args['records']:
        dynamo_data = record['dynamodb']
        new_image, old_image, keys = dynamo_data['NewImage'], dynamo_data['OldImage'], dynamo_data['Keys']
        disposition = new_image.get('disposition')
        if disposition == {'S': 'graphing'}:
            logging.info(f'based on the disposition of the NewImage, object should be pushed to the graph')
            orders.send_mail('graphing', keys)
    orders.close()
