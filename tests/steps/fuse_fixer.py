from assertpy import assert_that
from behave import *
from mock import patch

from toll_booth.alg_obj.aws.aws_obj.lockbox import FuseFixer


@when('the fuse fixer is run for "{identifier}"')
def step_impl(context, identifier):
    """
    :param identifier:
    :type context: behave.runner.Context
    """
    with patch('redis.client.Redis.scan') as mock_scan, \
            patch('redis.client.Redis.delete') as mock_delete:
        scans = [(1, [1, 2, 3]), (1, [4, 5, 6]), (9, [1]), (0, [])]
        mock_scan.side_effect = scans
        context.scan_cycles = len([x for x in scans if x[1] != []])
        kwargs = {}
        if identifier == 'a given object type':
            identifier = {'id_source': 'Algernon', 'object_type': 'ExternalId', 'id_name': 'emp_id'}
            kwargs['identifier'] = identifier
        fixer = FuseFixer(**kwargs)
        fixer.reset_fuses()
        context.mock_scan = mock_scan
        context.mock_delete = mock_delete


@then('fuses for "{identifier}" are deleted')
def step_impl(context, identifier):
    """
    :param identifier:
    :type context: behave.runner.Context
    """
    expected_scan_key = 'monitoring.*'
    if identifier == 'a given object type':
        expected_scan_key = 'monitoring.Algernon.ExternalId.emp_id.*'
    mock_scan = context.mock_scan
    mock_delete = context.mock_delete
    scan_calls = mock_scan.call_args_list
    scanned_keys = set(x[1]['match'] for x in scan_calls)
    assert_that(scanned_keys).is_length(1)
    assert_that(scanned_keys).contains(expected_scan_key)
    assert_that(mock_delete.called).is_true()
    assert_that(mock_delete.call_count).is_equal_to(context.scan_cycles)
