redis_pipeline_patch = 'redis.client.BasePipeline.execute'
redis_load_patch = 'redis.client.BasePipeline.execute_command'
redis_client_patch = 'redis.client.Redis.execute_command'
boto_patch = 'botocore.client.BaseClient._make_api_call'
send_patch = 'toll_booth.alg_obj.forge.comms.queues.OrderSwarm.send'
add_patch = 'toll_booth.alg_obj.forge.comms.queues.OrderSwarm.add_order'
dynamo_driver_stage_patch = 'toll_booth.alg_obj.aws.aws_obj.dynamo_driver.DynamoDriver.mark_object_as_stage_cleared'
dynamo_driver_write_patch = 'toll_booth.alg_obj.aws.aws_obj.dynamo_driver.DynamoDriver.write_vertex'
requests_patch = 'requests.sessions.Session.post'
requests_get_patch = 'requests.sessions.Session.get'
x_ray_patch_begin = 'aws_xray_sdk.core.recorder.AWSXRayRecorder.begin_subsegment'
x_ray_patch_end = 'aws_xray_sdk.core.recorder.AWSXRayRecorder.end_subsegment'
