from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_tasks.rivers.rocks import task


@xray_recorder.capture('transform')
@task('transform')
def transform(**kwargs):
    from toll_booth.alg_obj.graph.ogm.arbiter import RuleArbiter

    from toll_booth.alg_obj.graph.ogm.regulators import VertexRegulator

    schema_entry = kwargs['schema_entry']
    extracted_data = kwargs['extracted_data']
    vertex_regulator = VertexRegulator(schema_entry)
    source_vertex = vertex_regulator.create_potential_vertex(extracted_data['source'])
    arbiter = RuleArbiter(source_vertex, schema_entry)
    potentials = arbiter.process_rules(extracted_data)
    return {'potentials': potentials}


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


@xray_recorder.capture('load')
@task('load')
def load(**kwargs):
    from toll_booth.alg_obj.graph.ogm.ogm import Ogm

    ogm = Ogm(**kwargs)


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
