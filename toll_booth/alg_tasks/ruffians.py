import logging
from datetime import timedelta
import time

from botocore.exceptions import ReadTimeoutError

from toll_booth.alg_tasks.lambda_logging import lambda_logged


@lambda_logged
def decide(event, context):
    from toll_booth.alg_obj.aws.gentlemen.command import General

    abort_seconds = event.get('abort_seconds', 60)
    abort_level = (timedelta(seconds=abort_seconds).seconds * 1000)
    attempts = 0
    max_attempts = event.get('max_attempts', 3)
    general_args = {
        'domain_name': event['domain_name'],
        'task_list': event['decider_list']
    }
    time_remaining = context.get_remaining_time_in_millis()
    while time_remaining >= abort_level:
        while attempts <= max_attempts:
            general = General(**general_args)
            try:
                general.command()
            except ReadTimeoutError:
                attempts += 0
            except Exception as e:
                import traceback
                trace = traceback.format_exc()
                logging.error(f'error occurred in the decide task: {e}, trace: {trace}')


@lambda_logged
def labor(event, context):
    from multiprocessing import Pool

    def _labor(laborer_name, domain_name, task_list):
        from toll_booth.alg_obj.aws.gentlemen.labor import Laborer

        attempts = 0
        max_attempts = event.get('max_attempts', 3)
        while attempts <= max_attempts:
            laborer = Laborer(laborer_name, domain_name, task_list)
            try:
                laborer.labor()
            except ReadTimeoutError:
                attempts += 1
            except Exception as e:
                import traceback
                trace = traceback.format_exc()
                logging.error(f'error occurred in the labor task: {e}, trace: {trace}')

    warn_level = (timedelta(seconds=event.get('warn_seconds', 45)).seconds * 1000)
    abort_level = (timedelta(seconds=event.get('abort_seconds', 45)).seconds * 1000)
    work_lists = event['work_lists']
    number_threads = sum([x for x in work_lists.values()])
    with Pool(number_threads) as pool:
        results = {}
        for task_list_name, num_threads in work_lists.items():
            results[task_list_name] = {}
            for _ in range(number_threads):
                name = f'{task_list_name}-{_}'
                result = pool.apply_async(_labor, args=(name, event['domain_name'], task_list_name))
                results[task_list_name][name] = result
        time_remaining = context.get_remaining_time_in_millis()
        while time_remaining >= abort_level:
            for task_list_name, laborers in results.items():
                for name, labor_result in laborers.items():
                    if labor_result.ready() and time_remaining >= warn_level:
                        labor_result = pool.apply_async(_labor, args=(name, event['domain_name'], task_list_name))
                        results[task_list_name][name] = labor_result
            time.sleep(15)
            time_remaining = context.get_remaining_time_in_millis()
        pool.close()
        pool.join()
