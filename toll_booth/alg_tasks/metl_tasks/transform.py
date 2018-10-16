from toll_booth.alg_obj.forge.robot_in_disguise import DisguisedRobot
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
def transform(*args, **kwargs):
    print('starting a transform task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    disguised_robot = DisguisedRobot(**task_args)
    return disguised_robot.transform()
