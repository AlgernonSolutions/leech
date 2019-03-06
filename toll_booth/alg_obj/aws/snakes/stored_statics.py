import base64
import os

import boto3


class StaticAsset:
    def __init__(self, asset_name, asset_remote_path, stored_asset=None, **kwargs):
        bucket_name = kwargs.get('bucket_name')
        folder_name = kwargs.get('folder_name')
        if not bucket_name:
            bucket_name = os.getenv('ASSET_BUCKET', 'algernonsolutions-leech')
        if not folder_name:
            folder_name = os.getenv('ASSET_FOLDER', 'statics')
        self._asset_name = asset_name
        self._asset_remote_path = asset_remote_path
        self._bucket_name = bucket_name
        self._folder_name = folder_name
        self._stored_asset = stored_asset

    def _retrieve(self):
        resource = boto3.resource('s3')
        stored_asset_object = resource.Object(self._bucket_name, self.data_key).get()
        stored_asset = stored_asset_object['Body'].read()
        return stored_asset

    @property
    def data_key(self):
        return f'{self._folder_name}/{self._asset_name}'

    @property
    def bucket_name(self):
        return self._bucket_name

    @property
    def folder_name(self):
        return self._folder_name

    @property
    def stored_asset(self):
        if not self._stored_asset:
            stored_asset = self._retrieve()
            self._stored_asset = stored_asset
        return self._stored_asset


class StaticImage(StaticAsset):
    def __init__(self, **kwargs):
        folder_name = kwargs.get('folder_name')
        if not folder_name:
            folder_name = os.getenv('ASSET_FOLDER', 'statics/images')
        super().__init__(**kwargs, folder_name=folder_name)

    @classmethod
    def for_algernon_logo_large(cls, **kwargs):
        asset_name = 'alg_final_large.jpg'
        return cls(**kwargs, asset_name=asset_name, asset_remote_path=asset_name)

    @classmethod
    def for_algernon_logo_small(cls, **kwargs):
        asset_name = 'alg_final_small.jpg'
        return cls(**kwargs, asset_name=asset_name, asset_remote_path=asset_name)

    @property
    def stored_asset(self):
        return base64.b64decode(super().stored_asset)

    def __str__(self):
        return self._stored_asset
