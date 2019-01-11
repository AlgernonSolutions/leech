import json
import os

import boto3


class RuffianName:
    def __init__(self, flow_id, run_id):
        self._flow_id = flow_id
        self._run_id = run_id

    @property
    def name(self):
        return f'{self._flow_id}-{self._run_id}'

    def __str__(self):
        return self.name


class Ruffian:
    @classmethod
    def conscript(cls, decider_list, work_lists, domain_name, is_vpc=False, **kwargs):
        default_machine_arn = os.getenv('RUFFIAN_MACHINE', '')
        if is_vpc:
            default_machine_arn = os.getenv('VPC_RUFFIAN_MACHINE', '')
        machine_arn = kwargs.get('machine_arn', default_machine_arn)
        client = boto3.client('sfn')
        machine_input = json.dumps({
            'decider_list': decider_list,
            'work_lists': work_lists,
            'domain_name': domain_name
        })
        response = client.start_execution(
            stateMachineArn=machine_arn,
            input=machine_input
        )
        execution_arn = response['execution_arn']
        return execution_arn

    @classmethod
    def disband(cls, execution_arn):
        client = boto3.client('sfn')
        client.stop_execution(
            executionArn=execution_arn
        )
