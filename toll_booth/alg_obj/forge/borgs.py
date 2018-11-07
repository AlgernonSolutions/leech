from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.sapper.dynamo_driver import LeechDriver
from toll_booth.alg_obj.graph.ogm.regulators import EdgeRegulator


class SevenOfNine:
    def __init__(self, metal_order, **kwargs):
        self._assimilate_order = metal_order
        self._source_vertex = metal_order.source_vertex
        self._potential_vertex = metal_order.potential_vertex
        self._rule_entry = metal_order.rule_entry
        self._extracted_data = metal_order.extracted_data
        self._dynamo_driver = LeechDriver()

    def assimilate(self):
        assimilation_results = []
        edge_regulator = EdgeRegulator.get_for_object_type(self._rule_entry.edge_type)
        identified_vertexes, exist = self._derive_vertexes()
        for vertex in identified_vertexes:
            edge = self._derive_edge(edge_regulator, vertex)
            assimilation_results.append({
                'edge': edge,
                'vertex': vertex
            })
        self._write_assimilation_results(identified_vertexes, exist, assimilation_results)

    def _derive_vertexes(self):
        if self._potential_vertex.is_properties_complete and self._potential_vertex.is_identifiable:
            return [self._potential_vertex], False
        found_vertexes = self._dynamo_driver.find_potential_vertexes(
            self._potential_vertex.object_type, self._potential_vertex.object_properties)
        if found_vertexes:
            return found_vertexes, True
        if self._rule_entry.is_stub:
            return [self._potential_vertex], False
        return [], None

    def _write_assimilation_results(self, identified_vertexes, exist, assimilation_results):
        self._set_new_vertexes(identified_vertexes, exist)
        self._set_assimilation_results(assimilation_results)

    def _set_new_vertexes(self, identified_vertexes, exist):
        if exist:
            return
        if self._rule_entry.is_stub:
            return self._write_vertexes(identified_vertexes, True)
        if self._rule_entry.is_create:
            return self._write_vertexes(identified_vertexes)
        if self._rule_entry.is_pass:
            return
        raise NotImplementedError('do not know what to do with potential_vertex: %s for rule_type %s' % (
            self._potential_vertex, self._rule_entry.if_missing))

    def _derive_edge(self, edge_regulator, potential_vertex):
        return edge_regulator.generate_potential_edge(
            self._source_vertex, potential_vertex, self._extracted_data, self._rule_entry.inbound)

    def _set_assimilation_results(self, assimilation_results):
        try:
            self._dynamo_driver.set_assimilation_results(
                self._rule_entry.edge_type, assimilation_results,
                identifier_stem=self._source_vertex.identifier_stem,
                id_value=self._source_vertex.id_value)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e

    def _write_vertexes(self, vertexes, is_stub=False):
        for vertex in vertexes:
            self._write_vertex(vertex, is_stub)

    def _write_vertex(self, vertex, is_stub):
        try:
            if is_stub:
                return self._dynamo_driver.set_assimilated_vertex(vertex, is_stub, identifier_stem=None, id_value=None)
            return self._dynamo_driver.set_assimilated_vertex(
                vertex, is_stub, identifier_stem=vertex.identifier_stem, id_value=vertex.id_value)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
