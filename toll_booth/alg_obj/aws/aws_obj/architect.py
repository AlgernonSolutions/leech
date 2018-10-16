import configparser
import os

import boto3

from toll_booth.alg_obj import AlgObject


class Architect(AlgObject):
    def __init__(self, config_bucket_name=None, config_file_key=None):
        if not config_bucket_name:
            config_bucket_name = os.environ['CONFIG_BUCKET']
        if not config_file_key:
            config_file_key = os.environ['CONFIG_KEY']
        resource = boto3.resource('s3')
        bucket_resource = resource.Bucket(config_bucket_name)
        config_file = bucket_resource.Object(config_file_key).get()
        config_body = config_file['Body'].read().decode()
        config = configparser.ConfigParser()
        config.read_string(config_body)
        for section in config.sections():
            for key, value in config.items(section):
                os.environ[key.upper()] = value
        self._config = config
