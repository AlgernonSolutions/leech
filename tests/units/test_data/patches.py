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
x_ray_patch_other = 'aws_xray_sdk.core.recorder.AWSXRayRecorder.record_subsegment'
neptune_patch = 'toll_booth.alg_obj.aws.trident.connections.TridentNotary.send'
leech_driver_base = 'toll_booth.alg_obj.aws.sapper.dynamo_driver.LeechDriver'
schema_entry_patch = 'toll_booth.alg_obj.graph.schemata.schema_entry.SchemaEntry'
version_path = 'toll_booth.alg_obj.aws.gentlemen.tasks.Versions._retrieve'
config_path = 'toll_booth.alg_obj.aws.gentlemen.tasks.LeechConfig.get'
decision_polling = 'toll_booth.alg_obj.aws.gentlemen.command.General._poll_for_decision'
activity_polling = 'toll_booth.alg_obj.aws.gentlemen.labor.Laborer.poll_for_tasks'

base_paths = {
    'leech_driver': 'toll_booth.alg_obj.aws.sapper.dynamo_driver.LeechDriver',
    'edge_regulator': 'toll_booth.alg_obj.graph.ogm.regulators.EdgeRegulator',
    'schema_entry': schema_entry_patch
}


def get_version_patch():
    return patch(version_path)


def get_config_patch():
    return patch(config_path)


def get_poll_patch(for_decision=False):
    if for_decision:
        return patch(decision_polling)
    return patch(activity_polling)


def get_boto_patch():
    return patch(boto_patch)


def get_function_patch(base_name, function_name):
    return patch(f'{base_paths[base_name]}.{function_name}')


def get_leech_driver_patch(function_name):
    return patch(f'{leech_driver_base}.{function_name}')
