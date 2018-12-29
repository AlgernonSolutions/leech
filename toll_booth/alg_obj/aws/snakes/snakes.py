import json
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder


class StoredData(AlgObject):
    def __init__(self, data_name, data_string, bucket_name=None, timestamp=None, full_unpack=False):
        if not bucket_name:
            bucket_name = os.getenv('SNAKE_BUCKET', 'algernonsolutions-snakes')
        if not timestamp:
            timestamp = str(datetime.utcnow().timestamp())
        self._data_name = data_name
        self._data_string = data_string
        self._bucket_name = bucket_name
        self._timestamp = timestamp
        self._full_unpack = full_unpack

    @property
    def to_json(self):
        if not self.check:
            self.store()
        return {'pointer': self.pointer}

    @property
    def check(self):
        resource = boto3.resource('s3')
        object_resource = resource.Object(self._bucket_name, self.data_key)
        try:
            object_resource.load()
        except ClientError as e:
            return int(e.response['Error']['Code']) != 404
        return True

    @property
    def pointer(self):
        return f'{self._bucket_name}#{self.data_key}'

    @property
    def data_key(self):
        return f'{self._data_name}!{self._timestamp}'

    @property
    def data_string(self):
        return self._data_string

    @property
    def full_unpack(self):
        return self._full_unpack

    @classmethod
    def retrieve(cls, pointer):
        pointer_parts = pointer.split('#')
        key_parts = pointer_parts[1].split('!')
        resource = boto3.resource('s3')
        stored_object = resource.Object(pointer_parts[0], pointer_parts[1]).get()
        string_body = stored_object['Body'].read()
        body = json.loads(string_body, cls=AlgDecoder)
        return cls(key_parts[0], body['data_string'], pointer_parts[0], key_parts[1], body['full_unpack'])

    @classmethod
    def parse_json(cls, json_dict):
        pointer = json_dict['pointer']
        stored_data = cls.retrieve(pointer)
        if stored_data.full_unpack:
            return stored_data.data_string
        return stored_data

    @classmethod
    def from_object(cls, data_name, alg_object, full_unpack=False):
        return cls(data_name, alg_object, full_unpack=full_unpack)

    def store(self):
        if self.check:
            raise RuntimeError('can not overwrite stored data')
        resource = boto3.resource('s3')
        body = {'data_string': self._data_string, 'full_unpack': self._full_unpack}
        body_string = json.dumps(body, cls=AlgEncoder)
        resource.Object(self._bucket_name, self.data_key).put(Body=body_string)
        return self.pointer
