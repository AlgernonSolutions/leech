import logging

from toll_booth.alg_obj.graph.ogm.santorini import Vinny, MailBoxes
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def explode(*args, **kwargs):
    logging.info(f'started explode task with args: {args}, kwargs: {kwargs}')
    orders = MailBoxes()
    task_args = kwargs['task_args']
    vinny = Vinny()
    for record in task_args['records']:
        results = vinny.rig_explosion(record)
        for result in results:
            orders.send_mail(*result)

    orders.close()
