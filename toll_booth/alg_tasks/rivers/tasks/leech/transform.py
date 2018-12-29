import logging

from toll_booth.alg_obj.forge.robot_in_disguise import DisguisedRobot


def transform(**kwargs):
    logging.info('starting a transform task with kwargs: %s/%s' % kwargs)
    disguised_robot = DisguisedRobot(**kwargs)
    return disguised_robot.transform()
