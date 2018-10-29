import json
import re
import urllib.parse

from behave import *
from grappa import should

from tests.steps.actor_setup.assimilator import MockAssimilator
from tests.steps.actor_setup.extractor import MockExtractor
from tests.steps.actor_setup.loader import MockLoader
from tests.steps.actor_setup.monitor import MockMonitor
from tests.steps.actor_setup.transformer import MockTransformer
from toll_booth.alg_obj.forge.comms.orders import TransformObjectOrder, AssimilateObjectOrder, ExtractObjectOrder
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex
from toll_booth.alg_obj.serializers import AlgDecoder


@given("the test schema exists")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    from tests.steps.schema_setup import get_test
    test_schema = get_test()
    context.schema = test_schema


@given('a collection of objects in a remote data source "{trial_number}"')
def step_impl(context, trial_number):
    """
    :param trial_number:
    :type context: behave.runner.Context
    """

    for row in context.table:
        row_number = row['#']
        if row_number == trial_number:
            id_type = row['id type']
            if id_type == 'None':
                id_type = None
            trial = {
                'method': row['extraction method'],
                'source': row['source name'],
                'object_type': row['object type'],
                'id_type': id_type,
                'id_name': row['id name']
            }
            break
    else:
        raise RuntimeError(f'could not find a matching trial for number: {trial_number}')
    context.active_trial = trial_number
    context.active_params = trial


@when("new objects are created in the remote data sources")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    context.remote_maximum_value = 1005
    context.local_maximum_value = 1001


@then("the monitor will detect the new objects and start them in the pipeline")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    MockMonitor.run(context)
    monitor_mocks = context.monitor_mocks
    swarm_add = monitor_mocks['add']
    swarm_send = monitor_mocks['send']
    assert len(swarm_add.call_args_list) == (context.remote_maximum_value - context.local_maximum_value + 1)
    assert swarm_send.called is True
    context.extraction_orders = _assert_order_generation(context, swarm_add, swarm_send, ExtractObjectOrder)


@then("the extractor will retrieve the remote information about a single object")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    extraction_orders = context.extraction_orders
    manifest = []
    for extraction_order in extraction_orders:
        context.extraction_order = extraction_order
        MockExtractor.run(context)
        extractor_mocks = context.extractor_mocks
        swarm_add = extractor_mocks['add']
        swarm_send = extractor_mocks['send']
        transform_orders = _assert_order_generation(context, swarm_add, swarm_send, TransformObjectOrder)
        manifest.append({'extraction': extraction_order, 'transforms': transform_orders})
    context.manifest = manifest


@then("the transformer will create the focus vertex for that object")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    assimilate_manifest = []
    for order_set in context.manifest:
        for transform_order in order_set['transforms']:
            context.transform_order = transform_order
            MockTransformer.run(context)
            transformer_mocks = context.transformer_mocks
            swarm_add = transformer_mocks['add']
            swarm_send = transformer_mocks['send']
            assimilate_orders = _assert_order_generation(context, swarm_add, swarm_send, AssimilateObjectOrder)
            assimilate_manifest.append(
                {'extraction': order_set['extraction'], 'transform': transform_order, 'assimilates': assimilate_orders})
    context.manifest = assimilate_manifest


@then("the assimilator will generate the edges needed to join the object to the graph")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    load_manifest = []
    for order_set in context.manifest:
        for assimilate_order in order_set['assimilates']:
            context.assimilate_order = assimilate_order
            MockAssimilator.run(context)
            mocks = context.assimilate_mocks
            swarm_add = mocks['add']
            swarm_send = mocks['send']
            load_orders = _assert_order_generation(context, swarm_add, swarm_send, LoadObjectOrder)
            load_manifest.append(
                {
                    'extract': order_set['extraction'], 'transform': order_set['transform'],
                    'assimilate': assimilate_order, 'loads': load_orders
                }
            )
    context.manifest = load_manifest


@then("the loader will push the objects to the storage engine")
def step_impl(context):
    """
    :type context: behave.runner.Context
    """
    results = []
    for order_set in context.manifest:
        for load_order in order_set['loads']:
            context.load_order = load_order
            MockLoader.run(context)
            results.append(
                {
                    'extract': order_set['extract'], 'transform': order_set['transform'],
                    'assimilate': order_set['assimilate'], 'load': load_order, 'mocks': context.assimilate_mocks
                }
            )
    context.results = results
    _assert_final_results(context)


def _assert_order_generation(context, mock_add, mock_send, expected_order_type):
    assert mock_send.called is True
    sent_orders = []
    for entry in mock_add.call_args_list:
        order = entry[0][0]['task_args']
        built_order = json.loads(order, cls=AlgDecoder)['metal_order']
        sent_orders.append(built_order)
    for sent_order in sent_orders:
        _assert_order_properties(context, sent_order, expected_order_type)
    try:
        return sent_orders
    except IndexError:
        return None


def _assert_order_properties(context, metal_order, order_type):
    metal_order | should.be.type.of(order_type)
    if order_type == ExtractObjectOrder:
        _verify_extract_order(context, metal_order)
    if order_type == TransformObjectOrder:
        _verify_transform_order(metal_order)
    if order_type == AssimilateObjectOrder:
        _verify_assimilate_order(metal_order)
    if order_type == LoadObjectOrder:
        _verify_load_order(metal_order)


def _verify_extract_order(context, extract_order):
    extract_order | should.be.instance.of(ExtractObjectOrder)
    extraction_properties = extract_order.extraction_properties
    extraction_sources = _collect_extraction_sources(context)
    extract_order | should.have.property('id_value').which.should.be.a(int)
    extraction_properties | should.have.keys('id_value', 'object_type', 'id_name', 'id_source')
    # noinspection PyUnresolvedReferences
    extraction_sources | should.contain(extract_order.extraction_source)


def _verify_transform_order(transform_order):
    transform_order | should.be.type.of(TransformObjectOrder)
    transform_order.extracted_data | should.have.key('source')


def _verify_assimilate_order(assimilate_order):
    assimilate_order | should.be.instance(AssimilateObjectOrder)
    source_vertex = assimilate_order.source_vertex
    source_vertex | should.all(
        should.be.instance.of(PotentialVertex),
        should.have.property('is_identifiable').which.should.be.true,
        should.have.property('object_properties').which.should.be.a(dict)
    )


def _verify_load_order(load_order):
    vertex = load_order.vertex
    vertex | should.all(
        should.be.instance.of(PotentialVertex),
        should.have.property('is_identifiable').which.should.be.true,
        should.have.property('object_properties').which.should.be.a(dict)
    )


def _collect_extraction_sources(context):
    schema = context.schema
    object_type = context.active_params['object_type']
    extraction_sources = set()
    for entry in schema['vertex']:
        if entry['vertex_name'] == object_type:
            for extract in entry['extract']:
                extraction_sources.add(extract['extraction_source'])
    return tuple(extraction_sources)


def _assert_final_results(context):
    results = _organize_call_history(context.results)
    local_id_value = context.local_maximum_value
    remote_id_value = context.remote_maximum_value
    len(results.keys()) | should.be.equal.to(len(range(local_id_value, remote_id_value + 1)))
    for id_value, call_histories in results.items():
        _assert_graph_call(call_histories)
        _assert_index_call(call_histories)


def _assert_graph_call(call_histories):
    graph_commands = []
    for call in call_histories:
        param_string = call['mocks']['request'].call_args[1]['params']
        sent_command_params = urllib.parse.parse_qs(param_string)
        sent_commands = sent_command_params['gremlin'][0]
        graph_commands.extend(sent_commands.split(';'))
    for graph_command in graph_commands:
        reg_str = r"g\.(V|E)\(('[a-f0-9]{32}')\)\.fold\(\)\.coalesce\(unfold\(\), add\1\('(?!(None))\w+(::stub)*'\)(" \
                  r"\.from\(g\.V\(('[a-f0-9]{32}')\)\)\.to\(g\.V\(('[a-f0-9]{32}')\)\))*((\.property\()((('[\w]+', " \
                  r")((\d+)|('[\w\s\-\:\.\+]*')))|(id, \2))\))+\)"
        test_reg = re.compile(reg_str)
        match_test = test_reg.match(graph_command)
        match_test | should.not_be.none


def _assert_index_call(call_histories):
    for call in call_histories:
        called_args = call['mocks']['redis_pipeline'].call_args_list
        sent_commands = [x[0] for x in called_args]
        for command in sent_commands:
            redis_command = command[0]
            index_key = command[1]
            redis_command | should.any(should.be.equal.to('SETNX'), should.be.equal.to('ZADD'))
            index_key | should.be.a(str)
            if redis_command == 'ZADD':
                modifier_index, score_index, key_index = 2, 3, 4
                if len(command) == 4:
                    score_index, key_index = 2, 3
                score = command[score_index]
                key = command[key_index]
                score | should.be.a(int)
                key | should.any(should.be.a(int), should.be.a(str))
                if score_index == 2:
                    continue
                modifier = command[modifier_index]
                modifier | should.be.equal.to('NX')
            if redis_command == 'SETNX':
                key = command[2]
                key | should.be.a(str)


def _organize_call_history(call_history):
    returned = {}
    for entry in call_history:
        extract_call = entry['extract']
        id_value = extract_call.id_value
        if id_value not in returned:
            returned[id_value] = []
        returned[id_value].append(entry)
    return returned
