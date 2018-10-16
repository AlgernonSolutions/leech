from behave import *

from tests.steps.finder_setup.finder import run_finder
from tests.steps.outside_setup.neptune import VertexResponse

use_step_matcher("re")


@when("the user requests a vertex with a given internal_id")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    mock_internal_id = VertexResponse.generic().internal_id
    object_type = 'Vertex'
    context.object_type = object_type
    context.find_results = run_finder(object_type, mock_internal_id, context)


@then("the engine will return that vertex's data")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    results = context.find_results
    print(results)
    raise NotImplementedError(u'STEP: Then the engine will return that vertex')


@when("the user requests an edge with a given internal_id")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    object_type = 'Edge'
    mock_internal_id = VertexResponse.generic().internal_id
    context.object_type = object_type
    context.find_results = run_finder(object_type, mock_internal_id, context)


@then("the engine will return that edge's data")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: Then the engine will return that edge')


@when("the user requests an edge or vertex that doesn't exist")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: When the user requests an edge or vertex that doesn\'t exist')


@then("the engine will return an empty response")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: Then the engine will return an empty response')