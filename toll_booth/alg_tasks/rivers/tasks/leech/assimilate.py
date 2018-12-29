import logging

from toll_booth.alg_obj.forge.borgs import SevenOfNine


def assimilate(**kwargs):
    logging.info('starting a transform task with args/kwargs: %s' % kwargs)
    seven = SevenOfNine(**kwargs)
    return seven.assimilate()
