from toll_booth.alg_obj.aws.gql.gql_client import GqlClient
from toll_booth.alg_tasks.lambda_logging import lambda_logged


@lambda_logged
def fire_task(event, context):
    fire_command = '''
        mutation startWorkflow($flow_name: String!, $flow_id: String!, $domain_name:String!, $input_string: String){
          workflow(flow_name: $flow_name, flow_id: $flow_id, domain_name: $domain_name, input_string: $input_string)
        }
    '''
    gql_client = GqlClient()
    variables = {
        'flow_name': event['flow_name'],
        'flow_id': event['flow_id'],
        'domain_name': event['domain_name'],
        'input_string': event.get('input_string', '')
    }
    results = gql_client.query(fire_command, variables)
    return results
