import logging

from toll_booth.alg_obj.aws.matryoshkas.matryoshka import Matryoshka
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def unpack_matryoshka(*args, **kwargs):
    logging.info('firing the unpack_matryoshka command, with args/kwargs: %s/%s' % (args, kwargs))
    logging.info('unpacking a matryoshka')

    matryoshka = Matryoshka.from_json(kwargs)
    return matryoshka.completed_results
