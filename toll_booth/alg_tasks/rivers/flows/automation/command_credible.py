from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.gentlemen.decisions import CompleteWork
from toll_booth.alg_obj.aws.gentlemen.rafts import ActivitySignature, chain
from toll_booth.alg_tasks.rivers.rocks import workflow


# @xray_recorder.capture('command_credible')
@workflow('command_credible')
def command_credible(**kwargs):
    decisions = kwargs['decisions']
    command_chain = _build_command_chain(**kwargs)
    results = command_chain(**kwargs)
    if results is None:
        return
    decisions.append(CompleteWork())


def _build_command_chain(task_args, **kwargs):
    signatures = []
    command_name = task_args.get_argument_value('command_name')
    command_args = task_args.get_argument_value('command_args')
    for command_arg in command_args:
        command_id = str(hash(str(command_args)))
        command = {'command': command_name, 'command_args': command_arg}
        new_task_args = task_args.replace_argument_value('run_credible_command', command)
        signatures.append(ActivitySignature(command_id, 'run_credible_command', task_args=new_task_args, **kwargs))
    return chain(*tuple(signatures))
