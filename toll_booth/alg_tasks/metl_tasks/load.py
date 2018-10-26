from toll_booth.alg_obj.aws.aws_obj.dynamo_driver import DynamoDriver
from toll_booth.alg_obj.graph.ogm.ogm import Ogm
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def load(*args, **kwargs):
    print('starting a load task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    dynamo_driver = DynamoDriver(**task_args)
    key_fields = task_args['keys']
    keys = {
        'identifier_stem': key_fields['identifier_stem']['S'],
        'id_value': key_fields['sid_value']['S']
    }
    potential_object = dynamo_driver.get_object(**keys)
    ogm = Ogm(**task_args)
    return ogm.graph_object(potential_object)
