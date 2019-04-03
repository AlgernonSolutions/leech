import hashlib
import sys

import boto3


def get_existing_layer_arn(layer_name):
    client = boto3.client('lambda')
    paginator = client.get_paginator('list_layers')
    iterator = paginator.paginate()
    for page in iterator:
        for entry in page['Layers']:
            if entry['LayerName'] == layer_name:
                latest_entry = entry['LatestMatchingVersion']
                return layer_name, latest_entry['Version']


def get_layer_version(layer_name, layer_version):
    client = boto3.client('lambda')
    response = client.get_layer_version(
        LayerName=layer_name,
        VersionNumber=int(layer_version)
    )
    layer_content = response['Content']
    return layer_content['CodeSha256'], layer_content['CodeSize']


def get_compiled_layer_content(zip_file_path):
    with open(zip_file_path) as zip_file:
        zip_hash = hashlib.sha256(zip_file.read())
        return zip_hash


def publish_layer_version(layer_name, zip_file_path):
    client = boto3.client('lambda')
    with open(zip_file_path) as layer_zip:
        response = client.publish_layer_version(
            LayerName=layer_name,
            Description='compiled dependencies for the leech engine',
            Content={
                'ZipFile': layer_zip.read()
            },
            CompatibleRuntimes=['python3.6']
        )
        return response['Version']


def set_layer_version_parameter(layer_name, layer_version):
    client = boto3.client('ssm')
    client.put_parameter(
        Name=f'{layer_name}_layer_version',
        Description='current operational lambda layer for the leech',
        Value=str(layer_version),
        Type='String',
        Overwrite=True
    )


if __name__ == '__main__':
    args = sys.argv
    provided_layer_name = args[1]
    compiled_layer_zip_path = args[2]
    existing_layer = get_existing_layer_arn(provided_layer_name)
    if existing_layer is None:
        new_version = publish_layer_version(provided_layer_name, compiled_layer_zip_path)
        set_layer_version_parameter(provided_layer_name, new_version)
        exit()
    layer_hash, layer_size = get_layer_version(*existing_layer)
    compiled_layer_hash, compiled_layer_size = get_compiled_layer_content(compiled_layer_zip_path)
    print()
