import json
import logging
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder


class StoredData(AlgObject):
    def __init__(self, data_name, data_string, bucket_name=None, folder_name=None, timestamp=None, full_unpack=False,
                 is_stored=None):
        if not bucket_name:
            bucket_name = os.getenv('LEECH_BUCKET', 'the-leech')
        if not folder_name:
            folder_name = os.getenv('CACHE_FOLDER', 'cache')
        if not timestamp:
            timestamp = str(datetime.utcnow().timestamp())
        self._data_name = data_name
        self._data_string = data_string
        self._bucket_name = bucket_name
        self._folder_name = folder_name
        self._timestamp = timestamp
        self._full_unpack = full_unpack
        self._is_stored = is_stored

    @property
    def to_json(self):
        if not self.check:
            self.store()
        return {'pointer': self.pointer}

    @property
    def check(self):
        if self._is_stored is True:
            return True
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
        return f'{self._folder_name}/{self._data_name}!{self._timestamp}.json'

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
        folder_data_name = key_parts[0].split('/')
        folder_name = folder_data_name[0]
        data_name = folder_data_name[1]
        timestamp = key_parts[1].replace('.json', '')
        resource = boto3.resource('s3')
        stored_object = resource.Object(pointer_parts[0], pointer_parts[1]).get()
        string_body = stored_object['Body'].read()
        body = json.loads(string_body, cls=AlgDecoder)
        cls_args = {
            'data_name': data_name,
            'data_string': body['data_string'],
            'bucket_name': pointer_parts[0],
            'folder_name': folder_name,
            'timestamp': timestamp,
            'full_unpack': body['full_unpack'],
            'is_stored': True
        }
        return cls(**cls_args)

    @classmethod
    def parse_json(cls, json_dict):
        pointer = json_dict['pointer']
        stored_data = cls.retrieve(pointer)
        if stored_data.full_unpack:
            return stored_data.data_string
        return stored_data

    @classmethod
    def from_object(cls, data_name, alg_object, full_unpack=False):
        if isinstance(alg_object, StoredData):
            logging.debug(f'tried to store a StoredData object within another stored data object, that was naughty, '
                          f'you will go to jail now. jk, we just bypassed the upload and handed the original back: {alg_object}')
            return alg_object
        return cls(data_name, alg_object, full_unpack=full_unpack)

    def store(self):
        if self.check:
            raise RuntimeError('can not overwrite stored data')
        self._overwrite_store()

    def _overwrite_store(self):
        resource = boto3.resource('s3')
        body = {'data_string': self._data_string, 'full_unpack': self._full_unpack}
        body_string = json.dumps(body, cls=AlgEncoder)
        resource.Object(self._bucket_name, self.data_key).put(Body=body_string)
        self._is_stored = True
        return self.pointer

    def __str__(self):
        return self.pointer

    def merge(self, other_stored_data):
        current_data = self._data_string.copy()
        for data_name, data_entry in other_stored_data.data_string.items():
            if data_name in current_data:
                current_data_entry = current_data[data_name]
                if not isinstance(current_data_entry, list):
                    current_data[data_name] = [current_data_entry]
                current_data[data_name].append(data_entry)
                continue
            current_data[data_name] = data_entry
        if current_data == self._data_string:
            return False
        self._data_string = current_data
        self._overwrite_store()
        return True
