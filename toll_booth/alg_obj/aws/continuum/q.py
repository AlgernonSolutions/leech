import json
import os

import boto3
from botocore.exceptions import ClientError


class QueueAlreadyExistsException(Exception):
    def __init__(self, queue_name):
        self._queue_name = queue_name

    @property
    def queue_name(self):
        return self._queue_name


class Q:
    @classmethod
    def create_task_queue(cls, task_list_name, **kwargs):
        task_worker_function = kwargs.get('task_worker_function', os.environ['TASK_WORKER_FUNCTION'])
        queue = cls.create_queue(task_list_name, is_fifo=False, **kwargs)
        queue_arn = cls.get_queue_attributes(queue.url)['QueueArn']
        cls.attach_function_to_queue(queue_arn, task_worker_function)

    @classmethod
    def create_queue(cls, queue_name, is_fifo=False, queue_key='alias/aws/sqs', redrive_config=None, **kwargs):
        sqs = boto3.resource('sqs')
        queue_attributes = {}
        if redrive_config is not False:
            if redrive_config is None:
                redrive_config = RedriveConfig(for_fifo=is_fifo)
            queue_attributes['RedrivePolicy'] = redrive_config.for_q
        if queue_key is not None:
            queue_attributes['KmsMasterKeyId'] = queue_key
        if is_fifo is True:
            queue_name = f'{queue_name}.fifo'
            queue_attributes['FifoQueue'] = 'true'
        try:
            queue = sqs.create_queue(
                QueueName=queue_name,
                Attributes=queue_attributes
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'QueueAlreadyExists':
                raise QueueAlreadyExistsException(queue_name)
            raise e
        return queue

    @classmethod
    def attach_function_to_queue(cls, queue_arn, function_name, **kwargs):
        client = boto3.client('lambda')
        client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=function_name,
            Enabled=kwargs.get('enabled', True),
            BatchSize=kwargs.get('batch_size', 10)
        )

    @classmethod
    def get_queue_attributes(cls, queue_url):
        client = boto3.client('sqs')
        queue = client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['ALL'])
        queue_attributes = queue['Attributes']
        queue_attributes['QueueUrl'] = queue_url
        return queue_attributes

    @classmethod
    def find_queue_attributes(cls, queue_name):
        client = boto3.client('sqs')
        response = client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        return cls.get_queue_attributes(queue_url)

    @classmethod
    def delete_task_queue(cls, task_list_name):
        queue_attributes = cls.get_queue_attributes(task_list_name)
        sqs_client = boto3.client('sqs')
        cls.delete_queue_event_mapping(queue_attributes['QueueArn'])
        sqs_client.delete_queue(QueueUrl=queue_attributes['QueueUrl'])

    @classmethod
    def delete_queue_event_mapping(cls, queue_arn):
        client = boto3.client('lambda')
        paginator = client.get_paginator('list_event_source_mappings')
        iterator = paginator.paginate(EventSourceArn=queue_arn)
        for page in iterator:
            for entry in page['EventSourceMappings']:
                mapping_uuid = entry['UUID']
                client.delete_event_source_mapping(UUID=mapping_uuid)

    @classmethod
    def send_task_reminder(cls, task_list_name, task_name, task_id):
        if task_list_name != 'credible':
            raise NotImplementedError(f'tried to send a task reminder for list_name: {task_list_name}, you have not written that bit yet')
        queue_url = os.environ['CREDIBLE_TASKS_URL']
        queue = boto3.resource('sqs').Queue(queue_url)
        queue.send_message(
            MessageBody=json.dumps({
                'task_name': task_name,
                'task_id': task_id
            })
        )


class RedriveConfig:
    def __init__(self, for_fifo=False, **kwargs):
        default_dead_letter_env_name = 'DEAD_LETTER_ARN'
        if for_fifo is True:
            default_dead_letter_env_name = 'FIFO_DEAD_LETTER_ARN'
        self._dead_letter_arn = kwargs.get('dead_letter_arn', os.environ[default_dead_letter_env_name])
        self._max_receive_count = kwargs.get('max_receive_count', 10)

    @property
    def dead_letter_arn(self):
        return self._dead_letter_arn

    @property
    def max_receive_count(self):
        return self._max_receive_count

    @property
    def for_q(self):
        return {
            'deadLetterTargetArn': self._dead_letter_arn,
            'maxReceiveCount': self._max_receive_count
        }
