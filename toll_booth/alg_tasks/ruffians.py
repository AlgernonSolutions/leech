import json

from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder
from toll_booth.alg_tasks.lambda_logging import lambda_logged


def rough_work(production_fn):
    def wrapper(event, context):
        event = json.loads(json.dumps(event, cls=AlgEncoder), cls=AlgDecoder)
        return production_fn(event, context)
    return wrapper


@lambda_logged
@rough_work
def decide(event, context):
    from toll_booth.alg_obj.aws.ruffians.ruffian import Ruffian

    if 'warn_seconds' not in event:
        event['warn_seconds'] = 60
    event['work_lists'] = event['decider_list']
    ruffian = Ruffian.build(context, **event)
    ruffian.supervise()


@lambda_logged
@rough_work
def labor(event, context):
    from toll_booth.alg_obj.aws.ruffians.ruffian import Ruffian

    ruffian = Ruffian.build(context, **event)
    ruffian.labor()
