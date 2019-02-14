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

    schema_entry = kwargs['schema_entry']
    schema = kwargs['schema']
    extracted_data = kwargs['extracted_data']
    vertex_regulator = VertexRegulator(schema_entry)
    source_vertex = vertex_regulator.create_potential_vertex(extracted_data['source'])
    arbiter = RuleArbiter(source_vertex, schema, schema_entry)
    potentials = arbiter.process_rules(extracted_data)
    return {'potentials': potentials, 'source_vertex': source_vertex}


@xray_recorder.capture('assimilate')
@task('assimilate')
def assimilate(**kwargs):
    from toll_booth.alg_obj.graph.ogm.regulators import EdgeRegulator
    from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver

    extracted_data = kwargs['extracted_data']
    rule_entry = kwargs['rule_entry']
    source_vertex = kwargs['source_vertex']
    potential_vertex = kwargs['potential_vertex']
    leech_driver = LeechDriver(**kwargs)
    assimilation_results = []
    edge_regulator = EdgeRegulator.get_for_object_type(rule_entry.edge_type)
    identified_vertexes, exist = _derive_vertexes(potential_vertex, rule_entry, leech_driver)
    for vertex in identified_vertexes:
        edge_args = (source_vertex, potential_vertex, extracted_data, rule_entry.inbound)
        edge = edge_regulator.generate_potential_edge(*edge_args)
        assimilation_results.append({'edge': edge, 'vertex': vertex})
    return {'assimilation': assimilation_results}


@xray_recorder.capture('index')
@task('index')
def index(**kwargs):
    from toll_booth.alg_obj.graph.index_manager.index_manager import IndexManager

    schema_entry = kwargs['schema_entry']
    assimilation_results = kwargs['assimilation']
    source_vertex = kwargs['source_vertex']
    index_manager = IndexManager.from_graph_schema(schema_entry, **kwargs)
    index_manager.index_object(source_vertex)
    for entry in assimilation_results:
        index_manager.index_object(entry['edge'])
        index_manager.index_object(entry['vertex'])


@xray_recorder.capture('graph')
@task('graph')
def graph(**kwargs):
    from toll_booth.alg_obj.graph.ogm.ogm import Ogm

    ogm = Ogm(**kwargs)
    raise NotImplementedError('have not done the graph operation you chuckle head')


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
