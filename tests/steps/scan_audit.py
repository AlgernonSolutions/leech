import os

from behave import *
from mock import patch

from tests.steps.actor_setup import patches
from toll_booth.alg_obj.graph.auditing.scan_auditor import ScanAuditor, AuditTarget

use_step_matcher("re")


@given("a data space")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    os.environ['CACHE_PORT'] = '6379'
    os.environ['CACHE_URL'] = 'master.graph-cached.lq37xt.use1.cache.amazonaws.com'
    os.environ['NEPTUNE_ENDPOINT'] = 'third-neptune-test-cluster.cluster-cnv3iqiknsnm.us-east-1.neptune.amazonaws.com'
    os.environ[
        'GRAPH_READER_ENDPOINT'] = 'third-neptune-test-cluster.cluster-ro-cnv3iqiknsnm.us-east-1.neptune.amazonaws.com'
    os.environ['AUDIT_URL'] = 'https://sqs.us-east-1.amazonaws.com/803040539655/audit'


@given("the data space has some index-graph inconsistencies")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.inconsistent = True


@given("the data space has some missing objects")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: Given the data space has some missing objects')


@when("the data space is audited")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    audit_properties = {'id_source': 'MBI', 'id_type': 'Employees', 'id_name': 'emp_id'}
    with patch(patches.x_ray_patch_begin), patch(patches.x_ray_patch_end):
        audit_target = AuditTarget('ExternalId', audit_properties)
        scan_auditor = ScanAuditor(audit_target, segment_size=5)
        results = scan_auditor.segment_graph()
    print()


@when("the user specifies an object type to audit")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.object_type = 'ExternalId'


@then("the inconsistent objects are reprocessed")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: Then the inconsistent objects are reprocessed')


@then("the missing objects are reprocessed")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: Then the missing objects are reprocessed')


@then("only the specified object type is scanned")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    raise NotImplementedError(u'STEP: Then only the specified object type is scanned')
