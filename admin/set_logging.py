import logging


def set_logging():
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging.basicConfig(format='[%(levelname)s] %(asctime)s %(message)s', level=logging.INFO)
    logging.getLogger('botocore').setLevel(logging.WARNING)
