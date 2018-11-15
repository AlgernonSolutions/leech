import json
import logging
import os

import boto3

from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder


class RemoteFunctionExecutionException(Exception):
    def __init__(self, function_name, function_args, status_code, function_error, log_result):
        message = f'attempted a function call for function named: {function_name}, ' \
                  f'with arguments: {function_args}, but something went wrong, ' \
                  f'status_code: {status_code}, function_error: {function_error}, log_results: {log_result}'
        super().__init__(message)
        self._function_name = function_name
        self._function_args = function_args
        self._status_code = status_code
        self._function_error = function_error
        self._log_result = log_result

    @classmethod
    def generate(cls, function_name, function_args, lambda_response):
        return cls(
            function_name, function_args, lambda_response['StatusCode'],
            lambda_response['FunctionError'], lambda_response['LogResult']
        )


class StageManager:
    @classmethod
    def _run(cls, function_name, step_args, payload):
        client = boto3.client('lambda')
        response = client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload, cls=AlgEncoder)
        )
        if response['StatusCode'] != 200:
            raise RemoteFunctionExecutionException.generate(function_name, step_args, response)
        result_stream = response['Payload'].read()
        return json.loads(result_stream, cls=AlgDecoder)

    @classmethod
    def run_index_query(cls, function_name, step_args):
        payload = {
            'step_name': 'index_query',
            'step_args': step_args
        }
        return cls._run(function_name, step_args, payload)

    @classmethod
    def run_extraction(cls, function_name, step_args):
        payload = {
            'step_name': 'extraction',
            'step_args': step_args
        }
        logging.info('started to run an extraction with payload: %s' % payload)
        results = cls._run(function_name, step_args, payload)
        results = json.loads(results, cls=AlgDecoder)
        logging.info('completed an extraction with payload: %s, results: %s' % (payload, results))
        return results

    @classmethod
    def run_monitoring_extraction(cls, function_name, step_args):
        payload = {
            'step_name': 'monitor_extraction',
            'step_args': step_args
        }
        return cls._run(function_name, step_args, payload)

    @classmethod
    def run_field_value_query(cls, function_name, step_args):
        payload = {
            'step_name': 'field_value_query',
            'step_args': step_args
        }
        return cls._run(function_name, step_args, payload)

    @classmethod
    def bulk_mark_ids_as_working(cls, id_values, identifier_stem, object_type, stage_name):
        from toll_booth.alg_obj.aws.matryoshkas.matryoshka import Matryoshka, MatryoshkaCluster
        batches = []
        entries = []
        counter = 0
        receipt = {}
        for id_value in id_values:
            if len(entries) >= 20:
                batches.append({'id_values': entries})
                entries = []
            counter += 1
            receipt[str(counter)] = id_value
            entries.append(id_value)
        if entries:
            batches.append({'id_values': entries})
        send_task_name = 'bulk_dynamo_write'
        lambda_arn = os.environ['WORK_FUNCTION']
        task_constants = {
            'identifier_stem': str(identifier_stem),
            'object_type': object_type,
            'stage_name': stage_name
        }
        m_cluster = MatryoshkaCluster.calculate_for_concurrency(
            10, send_task_name, lambda_arn, task_args=batches, task_constants=task_constants, max_m_concurrency=25)
        m = Matryoshka.for_root(m_cluster)
        agg_results = m.aggregate_results
        return agg_results
