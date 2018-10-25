from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.aws_obj.dynamo_driver import DynamoDriver
from toll_booth.alg_obj.graph.ogm.regulators import EdgeRegulator


class SevenOfNine:
    def __init__(self, metal_order, **kwargs):
        self._assimilate_order = metal_order
        self._source_vertex = metal_order.source_vertex
        self._potential_vertex = metal_order.potential_vertex
        self._rule_entry = metal_order.rule_entry
        self._extracted_data = metal_order.extracted_data
        self._dynamo_driver = DynamoDriver()

    def assimilate(self):
        edge_regulator = EdgeRegulator.get_for_object_type(self._rule_entry.edge_type, self._rule_entry)
        vertexes = self._derive_vertexes()
        for vertex in vertexes:
            edge = self._derive_edge(edge_regulator, vertex)
            self._write_edge(edge)
        self._note_potential_vertex()

    def _derive_vertexes(self):
        if self._potential_vertex.is_identifiable:
            return [self._potential_vertex]
        found_vertexes = self._dynamo_driver.find_potential_vertexes(self._potential_vertex.object_properties)
        if found_vertexes:
            return found_vertexes
        if self._rule_entry.is_stub:
            return [self._potential_vertex]
        return []

    def _note_potential_vertex(self):
        potential_vertex = self._potential_vertex
        if self._rule_entry.is_stub:
            return self._write_stub_vertex(potential_vertex)
        if self._rule_entry.is_create:
            return self._write_vertex(potential_vertex)
        if self._rule_entry.is_pass:
            return
        raise NotImplementedError(
            'do not know what to do with potential_vertex: %s for rule_type %s' % (
                potential_vertex, self._rule_entry.if_missing))

    def _derive_edge(self, edge_regulator, potential_vertex):
        return edge_regulator.generate_potential_edge(
            self._source_vertex, potential_vertex, self._extracted_data, self._rule_entry.inbound)

    def _write_stub_vertex(self, vertex):
        try:
            self._dynamo_driver.add_stub_vertex(
                vertex.object_type,
                vertex.object_properties,
                self._source_vertex.internal_id,
                self._rule_entry.edge_type
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e

    def _write_edge(self, edge):
        try:
            self._dynamo_driver.write_edge(edge, 'assimilation')
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e

    def _write_vertex(self, vertex):
        try:
            self._dynamo_driver.write_vertex(vertex, 'assimilation')
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
