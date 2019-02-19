import logging

from aws_xray_sdk.core import xray_recorder


from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('pull_schema_entry')
@task('pull_schema_entry')
def pull_schema_entry(**kwargs):
    from toll_booth.alg_obj.graph.schemata.schema import Schema
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    identifier_stem = IdentifierStem.from_raw(kwargs['identifier_stem'])
    schema = Schema.retrieve(**kwargs)
    schema_entry = schema[identifier_stem.object_type]
    return {'schema_entry': schema_entry, 'schema': schema}


@xray_recorder.capture('transform')
@task('transform')
def transform(**kwargs):
    from toll_booth.alg_obj.graph.ogm.arbiter import RuleArbiter

    from toll_booth.alg_obj.graph.ogm.regulators import VertexRegulator

    transform_results = []
    schema_entry = kwargs['schema_entry']
    schema = kwargs['schema']
    extracted_data = kwargs['extracted_data']
    vertex_regulator = VertexRegulator(schema_entry)
    for entry in extracted_data:
        source_vertex = vertex_regulator.create_potential_vertex(entry['source'])
        arbiter = RuleArbiter(source_vertex, schema, schema_entry)
        potentials = arbiter.process_rules(entry)
        transform_results.append({'potentials': potentials, 'source_vertex': source_vertex})
    return {'transform': transform_results}


@xray_recorder.capture('assimilate')
@task('assimilate')
def assimilate(**kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import EdgeRegulator
    from toll_booth.alg_obj.graph.index_manager.index_manager import IndexManager

    schema = kwargs['schema']
    extracted_data = kwargs['extracted_data']
    rule_entry = kwargs['rule_entry']
    source_vertex = kwargs['source_vertex']
    potential_vertex = kwargs['potential_vertex']
    index_manager = IndexManager.from_graph_schema(schema, **kwargs)
    assimilation_results = [{'vertex': source_vertex}]
    edge_regulator = EdgeRegulator(schema[rule_entry.edge_type])
    identified_vertexes, exist = _derive_vertexes(potential_vertex, rule_entry, index_manager)
    for vertex in identified_vertexes:
        edge_args = (source_vertex, potential_vertex, extracted_data, rule_entry.inbound)
        edge = edge_regulator.generate_potential_edge(*edge_args)
        assimilation_results.append({'edge': edge, 'vertex': vertex})
    return {'assimilation': assimilation_results}


@xray_recorder.capture('index')
@task('index')
def index(**kwargs):
    from toll_booth.alg_obj.graph.index_manager.index_manager import IndexManager
    from toll_booth.alg_obj.graph.index_manager.indexes import UniqueIndexViolationException

    schema = kwargs['schema']
    vertexes, edges = _standardize_assimilation_results(**kwargs)
    source_vertex = kwargs['source_vertex']
    index_manager = IndexManager.from_graph_schema(schema, **kwargs)
    try:
        index_manager.index_object(source_vertex)
    except UniqueIndexViolationException as e:
        logging.warning(
            f'vertex {source_vertex} has already been set to the index, so violated {e.index_name}, just so you know')
    for edge in edges:
        try:
            index_manager.index_object(edge)
        except UniqueIndexViolationException as e:
            logging.warning(f'edge {edge} has already been set to the index, so violated {e.index_name}, just so you know')
    for vertex in vertexes:
        try:
            index_manager.index_object(vertex)
        except UniqueIndexViolationException as e:
            logging.warning(f'vertex {vertex} has already been set to the index, so violated {e.index_name}, just so you know')


@xray_recorder.capture('graph')
@task('graph')
def graph(**kwargs):
    from toll_booth.alg_obj.graph.ogm.ogm import Ogm

    vertexes, edges = _standardize_assimilation_results(**kwargs)
    ogm = Ogm(**kwargs)
    results = ogm.graph_objects(vertexes=vertexes, edges=edges)
    return {'graph': results}


def _standardize_assimilation_results(**kwargs):
    vertexes = {}
    edges = {}
    assimilation_results = _recurse(kwargs['assimilation'])
    for result in assimilation_results:
        vertex, edge = result.get('vertex'), result.get('edge')
        if vertex:
            vertexes[vertex.internal_id] = vertex
        if edge:
            edges[edge.internal_id] = edge
    return [x for x in vertexes.values()], [x for x in edges.values()]


def _recurse(obj):
    results = []
    if isinstance(obj, dict):
        results.append(obj)
    if isinstance(obj, list):
        for entry in obj:
            results.extend(_recurse(entry))
    return results


def _derive_vertexes(potential_vertex, rule_entry, leech_driver):
    if potential_vertex.is_properties_complete and potential_vertex.is_identifiable:
        return [potential_vertex], False
    found_vertexes = leech_driver.find_potential_vertexes(
        potential_vertex.object_type, potential_vertex.object_properties)
    if found_vertexes:
        return found_vertexes, True
    if rule_entry.is_stub:
        return [potential_vertex], False
    return [], None
