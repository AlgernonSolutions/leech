from unittest.mock import patch

redis_pipeline_patch = 'redis.client.BasePipeline.execute'
redis_load_patch = 'redis.client.BasePipeline.execute_command'
redis_client_patch = 'redis.client.Redis.execute_command'
boto_patch = 'botocore.client.BaseClient._make_api_call'
send_patch = 'toll_booth.alg_obj.forge.comms.queues.OrderSwarm.send'
small_send_patch = 'toll_booth.alg_obj.forge.comms.queues.SmallSwarm.send'
small_add_patch = 'toll_booth.alg_obj.forge.comms.queues.SmallSwarm.add_order'
add_patch = 'toll_booth.alg_obj.forge.comms.queues.OrderSwarm.add_order'
requests_patch = 'requests.sessions.Session.post'
requests_get_patch = 'requests.sessions.Session.get'
x_ray_patch_begin = 'aws_xray_sdk.core.recorder.AWSXRayRecorder.begin_subsegment'
x_ray_patch_end = 'aws_xray_sdk.core.recorder.AWSXRayRecorder.end_subsegment'
neptune_patch = 'toll_booth.alg_obj.aws.trident.connections.TridentNotary.send'
leech_driver_base = 'toll_booth.alg_obj.aws.sapper.dynamo_driver.LeechDriver'

base_paths = {
    'leech_driver': 'toll_booth.alg_obj.aws.sapper.dynamo_driver.LeechDriver',
    'edge_regulator': 'toll_booth.alg_obj.graph.ogm.regulators.EdgeRegulator',
}


def get_function_patch(base_name, function_name):
    return patch(f'{base_paths[base_name]}.{function_name}')


def get_leech_driver_patch(function_name):
    return patch(f'{leech_driver_base}.{function_name}')
