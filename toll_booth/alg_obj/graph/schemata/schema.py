import os

import boto3
import jsonref
from jsonschema import validate

from toll_booth.alg_obj.aws.aws_obj.sapper import SchemaWhisperer
from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry, SchemaEdgeEntry


class RemoteSchemaFile(AlgObject):
    def __init__(self, schema_body):
        self._schema_body = schema_body

    @classmethod
    def get_validation_schema(cls, bucket_name=None, file_key=None):
        if not bucket_name:
            bucket_name = os.environ['SCHEMA_BUCKET']
        if not file_key:
            file_key = 'config/master_schema.json'
        resource = boto3.resource('s3')
        bucket_resource = resource.Bucket(bucket_name)
        schema_file = bucket_resource.Object(file_key).get()
        schema_body = jsonref.loads(schema_file['Body'].read().decode())
        return cls(schema_body)

    @classmethod
    def get_working_schema(cls, file_key, bucket_name=None):
        return cls.get_validation_schema(bucket_name, file_key)

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['schema_body'])

    @property
    def schema_body(self):
        return self._schema_body

    def __getitem__(self, item):
        return self._schema_body[item]


class Schema(AlgObject):
    def __init__(self, vertex_entries, edge_entries):
        self._vertex_entries = {
            x['entry_name']: SchemaVertexEntry.parse(x) for x in vertex_entries
        }
        self._edge_entries = {
            x['edge_label']: SchemaEdgeEntry.parse(x) for x in edge_entries
        }

    @property
    def vertex_entries(self):
        return self._vertex_entries

    @property
    def edge_entries(self):
        return self._edge_entries

    def __getitem__(self, item):
        try:
            return SchemaVertexEntry.parse(self._vertex_entries[item])
        except KeyError:
            return SchemaEdgeEntry.parse(self._edge_entries[item])

    @classmethod
    def get(cls):
        schema_writer = SchemaWhisperer()
        current_entries = schema_writer.get_schema()
        return cls(current_entries['vertex'], current_entries['edge'])

    @classmethod
    def post(cls, schema_bucket_name=None, schema_file_key=None):
        if not schema_bucket_name:
            schema_bucket_name = os.environ['SCHEMA_BUCKET']
        if not schema_file_key:
            try:
                schema_file_key = os.environ['SCHEMA_KEY']
            except KeyError:
                schema_file_key = 'config/schema.json'
        master_schema = RemoteSchemaFile.get_validation_schema(schema_bucket_name).schema_body
        working_schema = RemoteSchemaFile.get_working_schema(schema_file_key, schema_bucket_name).schema_body
        validate(working_schema, master_schema)
        with SchemaWhisperer() as writer:
            for vertex_entry in working_schema['vertex']:
                writer.write_schema_entry(vertex_entry)
            for edge_entry in working_schema['edge']:
                writer.write_schema_entry(edge_entry)
        return cls(working_schema['vertex'], working_schema['edge'])

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['vertex_entries'], json_dict['edge_entries'])
