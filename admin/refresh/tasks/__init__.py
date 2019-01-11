_defaults = {
    'lambda_role': 'arn:aws:iam::803040539655:role/swf-lambda',
    'workflow_timeout': '86400',
    'workflow_task_timeout': 'NONE',
    'task_run_timeout': 'NONE',
    'task_heart_timeout': '30',
    'task_waiting_timeout': 'NONE',
    'task_total_timeout': 'NONE',
    'child_policy': 'TERMINATE'
}


def _compare_properties(config_object, existing_object, *args):
    config_property = _get_property(config_object, *args)
    existing_property = _get_property(existing_object, *args)
    return config_property == existing_property


def _get_property(target_object, *args):
    for arg in args:
        if target_object is None:
            return None
        target_object = target_object.get(arg, None)
    return target_object


