import logging

from toll_booth.alg_obj.aws.aws_obj.lockbox import FuseFixer
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def reset_fuses(*args, **kwargs):
    logging.info(f'started a reset fuses request with args: {args}/ kwargs: {kwargs}')
    task_args = args[0]
    fixer = FuseFixer(**task_args)
    fixer.reset_fuses()
