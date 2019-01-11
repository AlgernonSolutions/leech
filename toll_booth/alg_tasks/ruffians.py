from toll_booth.alg_tasks.lambda_logging import lambda_logged


@lambda_logged
def decide(event, context):
    from toll_booth.alg_obj.aws.ruffians.ruffian import Ruffian

    if 'warn_seconds' not in event:
        event['warn_seconds'] = 60
    event['work_lists'] = event['decider_list']
    ruffian = Ruffian.build(context, **event)
    ruffian.supervise()


@lambda_logged
def labor(event, context):
    from toll_booth.alg_obj.aws.ruffians.ruffian import Ruffian

    ruffian = Ruffian.build(context, **event)
    ruffian.labor()
