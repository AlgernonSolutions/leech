import logging

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('run_credible_commands')
@task('run_credible_commands')
def run_credible_command(**kwargs):
    from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver

    logging.info(f'started run credible task with kwargs: {kwargs}')
    command = kwargs['command']
    command_args = kwargs['command_args']
    id_source = kwargs['id_source']
    credentials = kwargs.get('credible_credentials')
    if isinstance(credentials, list):
        credentials = credentials[-1]
    with CredibleFrontEndDriver(id_source, credentials=credentials) as driver:
        credible_function = getattr(driver, command)
        function_result = credible_function(**command_args)
        return {command: function_result, 'credible_credentials': driver.credentials}
