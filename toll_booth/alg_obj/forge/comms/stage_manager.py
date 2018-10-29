import json

import boto3


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
            Payload=json.dumps(payload)
        )
        if response['StatusCode'] != 200:
            raise RemoteFunctionExecutionException.generate(function_name, step_args, response)
        result_stream = response['Payload'].read()
        return json.loads(result_stream)

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
        return cls._run(function_name, step_args, payload)
