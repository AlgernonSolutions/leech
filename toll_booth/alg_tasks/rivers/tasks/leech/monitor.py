import logging


from toll_booth.alg_obj.forge.lizards import MonitorLizard


def monitor(**kwargs):
    logging.info('starting a monitoring task with kwargs: %s' % kwargs)
    logging.info('starting to build the lizard')
    lizard = MonitorLizard(**kwargs)
    logging.info('built the lizard')
    return lizard.monitor()
