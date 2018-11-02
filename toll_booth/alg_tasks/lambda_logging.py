import logging


def lambda_logged(lambda_function):
    def wrapper(*args):
        event = args[0]
        context = args[1]
        root = logging.getLogger()
        if root.handlers:
            for handler in root.handlers:
                root.removeHandler(handler)
        logging.basicConfig(format='[%(levelname)s] ||' +
                                   f'function_name:{context.function_name}|function_arn{context.invoked_function_arn}|request_id:{context.aws_request_id}' +
                                   '|| %(asctime)s %(message)s', level=logging.INFO)
        logging.getLogger('aws_xray_sdk').setLevel(logging.WARNING)
        logging.getLogger('botocore').setLevel(logging.WARNING)

        return lambda_function(event, context)

    return wrapper
