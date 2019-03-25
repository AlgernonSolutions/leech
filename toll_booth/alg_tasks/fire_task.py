from toll_booth.alg_obj.aws.gql.gql_client import RuffianGql
from toll_booth.alg_tasks.lambda_logging import lambda_logged


@lambda_logged
def fire_task(event, context):
    gql_client = RuffianGql()
    variables = {
        'flow_name': event['flow_name'],
        'flow_id': event['flow_id'],
        'domain_name': event['domain_name'],
        'input_string': event.get('input_string', '')
    }
    results = gql_client.start_workflow(**variables)
    return results
