import base64
import csv
import io
import os
import rapidjson

import boto3
import dateutil

from toll_booth.alg_obj.serializers import AlgDecoder


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


class StaticCsv(StaticAsset):
    def __init__(self, **kwargs):
        folder_name = kwargs.get('folder_name')
        header_data_types = kwargs.get('header', {})
        if not folder_name:
            folder_name = os.getenv('ASSET_FOLDER', 'statics/csv')
        super().__init__(**kwargs, folder_name=folder_name)
        self._parsed = False
        self._header_data_types = header_data_types
        self._header = []

    @classmethod
    def for_check_dates(cls, id_source, **kwargs):
        asset_name = f'{id_source}_check_dates.csv'
        header = {
            'Check Date': 'datetime', 'HR Date': 'datetime', 'Billing Date': 'datetime',
            'Sample Start': 'datetime', 'Sample End': 'datetime', 'Sample Date': 'datetime',
            'Send Date': 'datetime'
        }
        return cls(**kwargs, asset_name=asset_name, asset_remote_path=asset_name, header=header)

    @classmethod
    def for_pay_rates(cls, id_source, **kwargs):
        asset_name = f'{id_source}_pay_rates.csv'
        return cls(**kwargs, asset_name=asset_name, asset_remote_path=asset_name)

    @property
    def stored_asset(self):
        stored_asset = super().stored_asset
        if self._parsed:
            return stored_asset
        self._parse()
        return self._stored_asset

    def __getitem__(self, item):
        self._check_parse()
        first_column = self._header[0]
        items = [x for x in self._stored_asset if x[first_column] == item]
        if not items:
            raise KeyError(item)
        return items

    def __iter__(self):
        self._check_parse()
        return iter(self.stored_asset)

    def get(self, item, default):
        self._check_parse()
        try:
            return self[item]
        except KeyError:
            return default

    def get_by_index(self, index):
        self._check_parse()
        return self._stored_asset[index]

    def index(self, item):
        self._check_parse()
        for index_number, entry in enumerate(self._stored_asset):
            if entry == item:
                return index_number
        raise IndexError(item)

    def _check_parse(self):
        if not self._parsed:
            self._parse()

    def _parse(self):
        if self._parsed:
            return
        parsed_asset, header = self._parse_csv()
        self._parsed = True
        self._stored_asset = parsed_asset
        self._header = header

    def _parse_csv(self):
        stored_asset = super().stored_asset
        csv_string = stored_asset.decode()
        header = []
        csv_values = []
        with io.StringIO(csv_string, newline='\r\n') as csv_string:
            reader = csv.reader(csv_string, delimiter=',', quotechar='"')
            for row_number, row in enumerate(reader):
                if row_number is 0:
                    for entry in row:
                        header.append(entry)
                    continue
                row_entry = {}
                for header_index, entry in enumerate(row):
                    try:
                        header_name = header[header_index]
                    except IndexError:
                        raise RuntimeError(
                            'the returned data from a csv query contained insufficient information to create the table')
                    entry = self._set_data_type(header_name, entry)
                    row_entry[header_name] = entry
                csv_values.append(row_entry)
        return csv_values, header

    def _set_data_type(self, header_name, entry):
        data_type = self._header_data_types.get(header_name)
        if not data_type:
            return entry
        if entry is None:
            return None
        if data_type == 'datetime':
            entry = dateutil.parser.parse(entry)
        return entry


class StaticJson(StaticAsset):
    def __init__(self, **kwargs):
        folder_name = kwargs.get('folder_name')
        if not folder_name:
            folder_name = os.getenv('ASSET_FOLDER', 'statics/json')
        super().__init__(**kwargs, folder_name=folder_name)
        self._parsed = False

    @classmethod
    def for_team_data(cls, id_source, **kwargs):
        asset_name = f'{id_source}_teams.json'
        return cls(**kwargs, asset_name=asset_name, asset_remote_path=asset_name)

    @property
    def stored_asset(self):
        stored_asset = super().stored_asset
        if self._parsed:
            return stored_asset
        self._parse()
        return self._stored_asset

    def _parse(self):
        if self._parsed:
            return
        stored_asset = rapidjson.loads(super().stored_asset, object_hook=AlgDecoder.object_hook)
        self._stored_asset = stored_asset
        self._parsed = True

    def __getitem__(self, item):
        if not self._parsed:
            self._parse()
        return self._stored_asset[item]

    def get(self, item, default):
        try:
            self[item]
        except KeyError:
            return default
