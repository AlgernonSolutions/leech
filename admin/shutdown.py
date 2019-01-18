import boto3


def shutdown_state_machine(execution_arn, error=None, cause=None):
    client = boto3.client('stepfunctions')
    stop_args = {
        'executionArn': execution_arn
    }
    if error:
        stop_args['error'] = error
    if cause:
        stop_args['cause'] = cause
    client.stop_execution(**stop_args)


def get_running_machines(machine_arn):
    client = boto3.client('stepfunctions')
    paginator = client.get_paginator('list_executions')
    response_iterator = paginator.paginate(
        stateMachineArn=machine_arn,
        statusFilter='RUNNING'
    )
    for page in response_iterator:
        for execution in page['executions']:
            yield execution['executionArn']


if __name__ == '__main__':
    target_machine_arn = 'arn:aws:states:us-east-1:803040539655:stateMachine:decider'
    shutdown_cause = 'emergency shutdown'
    for running_execution in get_running_machines(target_machine_arn):
        shutdown_state_machine(running_execution, cause=shutdown_cause)
