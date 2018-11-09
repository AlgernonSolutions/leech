import json
import logging
import os
import threading
from queue import Queue

from toll_booth.alg_obj.serializers import ExplosionDecoder


class Vinny:
    def __init__(self):
        self._tests = [
            self._check_for_assimilated_vertex,
            self._check_for_new_vertex
        ]

    def rig_explosion(self, dynamo_record):
        results = []
        explosive_record = ExplosionRecord.from_dynamo_record(dynamo_record)
        for test in self._tests:
            test_results = test(explosive_record)
            if test_results:
                results.append(test_results)
        return results

    @classmethod
    def _check_for_new_vertex(cls, record):
        if not record.is_insert:
            return
        disposition = record.new_image.get('disposition')
        if disposition == 'graphing':
            logging.info(f'based on the disposition of the NewImage, object should be pushed to the graph')
            return 'graphing', record.keys

    @classmethod
    def _check_for_assimilated_vertex(cls, record):
        if not record.is_modify:
            return
        old_progress = cls._get_assimilation_progress(record.old_image)
        new_progress = cls._get_assimilation_progress(record.new_image)
        for edge_label, progress in new_progress.items():
            if progress is not True:
                return
        old_check = False
        for edge_label, progress in old_progress.items():
            if progress is False:
                old_check = True
        if not old_check:
            return
        return 'graphing', record.keys

    @classmethod
    def _get_assimilation_progress(cls, image):
        progress = {}
        potentials = image.get('potentials', {})
        for edge_label, potential in potentials.items():
            assimilated = potential.get('assimilated')
            progress[edge_label] = assimilated
        return progress


class ExplosionRecord:
    def __init__(self, event_type, keys, new_image=None, old_image=None):
        if not new_image:
            new_image = {}
        if not old_image:
            old_image = {}
        self._event_type = event_type
        self._keys = keys
        self._old_image = old_image
        self._new_image = new_image

    @classmethod
    def from_dynamo_record(cls, dynamo_record):
        event_type = dynamo_record['eventName']
        dynamo_data = dynamo_record['dynamodb']
        new_image = json.loads(json.dumps(dynamo_data.get('NewImage')), cls=ExplosionDecoder)
        keys = json.loads(json.dumps(dynamo_data.get('Keys')), cls=ExplosionDecoder)
        old_image = json.loads(json.dumps(dynamo_data.get('OldImage')), cls=ExplosionDecoder)
        return cls(event_type, keys, new_image, old_image)

    @property
    def is_modify(self):
        return self._event_type == 'MODIFY'

    @property
    def is_insert(self):
        return self._event_type == 'INSERT'

    @property
    def keys(self):
        return self._keys

    @property
    def new_image(self):
        return self._new_image

    @property
    def old_image(self):
        return self._old_image


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
        pass
