import os

import boto3


class Q:
    @classmethod
    def create_task_queue(cls, task_list_name, **kwargs):
        task_worker_function = kwargs.get('task_worker_function', os.environ['TASK_WORKER_FUNCTION'])
        queue = cls.create_queue(task_list_name, is_fifo=True, **kwargs)
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
        queue = sqs.create_queue(
            QueueName=queue_name,
            Attributes=queue_attributes
        )
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
