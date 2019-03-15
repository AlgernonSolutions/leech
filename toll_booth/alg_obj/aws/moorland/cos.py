import os

import boto3
from botocore.exceptions import ClientError


class __CloudEnvironment:
    def __getitem__(self, item):
        local_environ_value = os.getenv(item)
        if local_environ_value is not None:
            return local_environ_value
        client = boto3.client('ssm')
        try:
            cloud_value = client.get_parameter(Name=item)
            return cloud_value['Parameter']['Value']
        except ClientError as e:
            if e.response['Error']['Code'] != 'ParameterNotFound':
                raise e
            raise KeyError(item)

    def __setitem__(self, key, value):
        client = boto3.client('ssm')
        client.put_parameter(Type='String', Name=key, Value=value, Overwrite=True)

    def get_env(self, item, default=None):
        try:
            return self.__getitem__(item)
        except KeyError:
            return default


def getenv(item, default=None):
    return __CloudEnvironment().get_env(item, default)


environ = __CloudEnvironment()
