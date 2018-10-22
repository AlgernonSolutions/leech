from toll_booth.alg_obj.aws.aws_obj.dynamo_driver import DynamoDriver
from toll_booth.alg_obj.forge.comms.orders import LoadObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.graph.ogm.regulators import EdgeRegulator


class SevenOfNine:
    def __init__(self, metal_order, **kwargs):
        self._assimilate_order = metal_order
        self._load_queue = kwargs.get('load_queue', ForgeQueue.get_for_load_queue(**kwargs))
        self._source_vertex = metal_order.source_vertex
        self._potential_vertex = metal_order.potential_vertex
        self._rule_entry = metal_order.rule_entry
        self._extracted_data = metal_order.extracted_data
        self._driver = DynamoDriver()

    def assimilate(self):
        if self._source_vertex == self._potential_vertex:
            self._load_queue.add_order(LoadObjectOrder.for_source_vertex(self._source_vertex))
            return self._load_queue.push_orders()
        edge_regulator = EdgeRegulator.get_for_object_type(self._rule_entry.edge_type, self._rule_entry)
        potential_vertexes = self._derive_potential_vertexes()
        for potential_vertex in potential_vertexes:
            edge = self._derive_edge(edge_regulator, potential_vertex)
            load_order = LoadObjectOrder(potential_vertex, edge)
            self._load_queue.add_order(load_order)
        return self._load_queue.push_orders()

    def _derive_potential_vertexes(self):
        if self._potential_vertex.is_identifiable:
            return [self._potential_vertex]
        found_vertexes = self._driver.find_potential_vertexes(self._potential_vertex.object_properties)
        if found_vertexes:
            return found_vertexes
        if self._rule_entry.is_stub:
            return [self._potential_vertex]
        return []

    def _derive_edge(self, edge_regulator, potential_vertex):
        if self._source_vertex == potential_vertex:
            return None
        return edge_regulator.generate_potential_edge(
            self._source_vertex, potential_vertex, self._extracted_data, self._rule_entry.inbound)
