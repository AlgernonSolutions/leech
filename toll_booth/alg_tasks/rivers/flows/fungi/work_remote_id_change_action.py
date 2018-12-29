"""
    This flow is the terminal flow for the unholy FungalLeech process,
    by this point, we have extracted all the driving vertexes, and then propagated out the change_categories,
    followed by the change_types, then ultimately down to the actual change itself.
    The change itself may need additional information, called enrichment, which was not produced through the initial
    extraction. If so, that data is gathered, and finally the entire thing is collected together into a single package,
    which we can then transform, assimilate, etc.
"""

import json

from toll_booth.alg_obj.aws.gentlemen.decisions import StartActivity, CompleteWork
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder
from toll_booth.alg_tasks.rivers.rocks import workflow


@workflow
def work_remote_id_change_action(decisions, **kwargs):
    working = False
    enrichment = _get_enrichment_data(**kwargs)
    if enrichment is None:
        return
    change_data = _build_change_data(enrichment, **kwargs)
    if change_data is None:
        return
    if not working:
        decisions.append(CompleteWork(change_data))


def _get_enrichment_data(execution_id, decisions, activities, input_kwargs, **kwargs):
    id_value = input_kwargs['id_value']
    change_category = input_kwargs['category']
    action_name = input_kwargs['action_name']
    action_id = change_category.get_action_id(action_name)
    change_action = change_category[action_id]
    activity_name = f'enrich_data-{action_name}-{change_category}-{id_value}-{execution_id}'
    if change_action.is_static and change_action.has_details is False:
        return {'change_detail': {}, 'emp_ids': {}}
    if activity_name not in activities:
        flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
        decisions.append(
            StartActivity(activity_name, 'get_enrichment_for_change_action', flow_input, task_list_name='Credible'))
        return None
    activity = activities[activity_name]
    if not activity.is_complete:
        return None
    if activity.is_failed:
        fail_count = activities.get_activity_failed_count(activity.activity_id)
        if fail_count >= 3:
            raise RuntimeError('failure in an activity: %s' % activity_name)
        decisions.append(StartActivity.for_retry(activity))
        return None
    return json.loads(activity.results, cls=AlgDecoder)


def _build_change_data(enriched_data, activities, decisions, execution_id, input_kwargs, **kwargs):
    input_kwargs['enriched_data'] = enriched_data
    id_value = input_kwargs['id_value']
    change_category = input_kwargs['category']
    activity_name = f'work_remote_id_change_type-{change_category}-{id_value}-{execution_id}'
    if activity_name not in activities:
        flow_input = json.dumps(input_kwargs, cls=AlgEncoder)
        decisions.append(
            StartActivity(activity_name, 'work_remote_id_change_type', flow_input, task_list_name='Credible'))
        return None
    activity = activities[activity_name]
    if not activity.is_complete:
        return None
    if activity.is_failed:
        fail_count = activities.get_activity_failed_count(activity.activity_id)
        if fail_count >= 3:
            raise RuntimeError('failure in an activity: %s' % activity_name)
        decisions.append(StartActivity.for_retry(activity))
        return None
    return json.loads(activity.results, cls=AlgDecoder)
