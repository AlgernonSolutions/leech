"""
    the master flow for the weird bit of getting information out of a FIP ICAMS domain,
    this flow works by vertex driven growth,
    under this process, we are unable to access the unique identifiers of the object of interest,
    so we instead access them through an object we can get unique identifiers for, creating a two step process
"""

import json

from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder
from toll_booth.alg_tasks.rivers.rocks import workflow
from toll_booth.alg_obj.aws.gentlemen.decisions import StartActivity, RecordMarker


@workflow
def command_fungi(markers, **kwargs):
    execution_id = kwargs['execution_id']
    kwargs['names'] = {
        'local': f'get_local_ids-{execution_id}',
        'remote': f'get_remote_ids-{execution_id}',
        'put': f'put_new_ids-{execution_id}',
        'link': f'link_new_ids-{execution_id}',
        'unlink': f'unlink_old_ids-{execution_id}',
        'change_types': f'pull_change_types-{execution_id}'
    }
    if 'get_ids' not in markers:
        return get_ids(**kwargs)
    if 'manage_ids' not in markers:
        return manage_ids(**kwargs)
    if 'change_types' not in markers:
        return get_change_types(**kwargs)
    work_remote_ids(**kwargs)


def get_ids(names, decisions, activities, flow_input, **kwargs):
    get_remote_id_name = names['remote']
    get_local_id_name = names['local']
    working = False
    if get_remote_id_name not in activities:
        decisions.append(StartActivity(names['remote'], 'get_remote_ids', flow_input, version='3'))
        working = True
    if get_local_id_name not in activities:
        decisions.append(StartActivity(names['local'], 'get_local_ids', flow_input, version='3'))
        working = True
    if working:
        return
    get_remote_ids_operation = activities[get_remote_id_name]
    get_local_ids_operation = activities[get_local_id_name]
    if get_remote_ids_operation.is_complete and get_local_ids_operation.is_complete:
        decisions.append(RecordMarker('get_ids', 'completed'))
        return 'fire'


def manage_ids(decisions, names, activities, input_kwargs, **kwargs):
    get_local_ids_operation = activities[names['local']]
    get_remote_ids_operation = activities[names['remote']]
    put_new_ids_operation_name = names['put']
    local_id_values = json.loads(get_local_ids_operation.results, cls=AlgDecoder)
    remote_id_values = json.loads(get_remote_ids_operation.results, cls=AlgDecoder)
    if put_new_ids_operation_name not in activities:
        new_id_values = remote_id_values - local_id_values['all']
        input_kwargs['id_values'] = new_id_values
        flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
        decisions.append(StartActivity(names['put'], 'put_new_ids', flow_input, version='1'))
        return
    if activities[put_new_ids_operation_name].is_complete:
        local_linked_values = local_id_values['linked']
        working = False
        link_new_ids_operation_name = names['link']
        unlink_old_ids_operation_name = names['unlink']
        if link_new_ids_operation_name not in activities:
            newly_linked_id_values = remote_id_values - local_linked_values
            input_kwargs['id_values'] = newly_linked_id_values
            flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
            decisions.append(StartActivity(names['link'], 'link_new_ids', flow_input, version='2'))
            working = True
        if unlink_old_ids_operation_name not in activities:
            unlinked_id_values = local_linked_values - remote_id_values
            input_kwargs['id_values'] = unlinked_id_values
            flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
            decisions.append(StartActivity(names['unlink'], 'unlink_old_ids', flow_input, version='2'))
            working = True
        if working:
            return
        pending_activities = [activities[link_new_ids_operation_name], activities[unlink_old_ids_operation_name]]
        all_complete = True
        for activity in pending_activities:
            all_complete = activity.is_complete
        if all_complete:
            decisions.append(RecordMarker('manage_ids', 'completed'))
            return 'fire'
        for activity in pending_activities:
            if activity.is_failed:
                fail_count = activities.get_activity_failed_count(activity.activity_id)
                if fail_count >= 3:
                    raise RuntimeError('failure in an assigned activity')
                decisions.append(StartActivity.for_retry(activity))


def get_change_types(flow_input, names, decisions, activities, **kwargs):
    get_change_types_name = names['change_types']
    if get_change_types_name not in activities:
        decisions.append(StartActivity(names['change_types'], 'pull_change_types', flow_input, version='1'))
        return
    activity = activities[get_change_types_name]
    if activity.is_complete:
        decisions.append(RecordMarker('change_types', 'completed'))
        return 'fire'
    if activity.is_failed:
        fail_count = activities.get_activity_failed_count(activity.activity_id)
        if fail_count >= 3:
            raise RuntimeError('failure in an assigned activity')
        decisions.append(StartActivity.for_retry(activity))


def work_remote_ids(execution_id, input_kwargs, activities, names, decisions, **kwargs):
    get_remote_ids_operation = activities[names['remote']]
    remote_id_values = json.loads(get_remote_ids_operation.results, cls=AlgDecoder)
    for remote_id_value in remote_id_values:
        pass

