from toll_booth.alg_obj.aws.aws_obj.dynamo_driver import DynamoDriver
from toll_booth.alg_obj.forge.comms.orders import AssimilateObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.graph.ogm.arbiter import RuleArbiter
from toll_booth.alg_obj.graph.ogm.regulators import PotentialVertex


class DisguisedRobot:
    def __init__(self, metal_order, **kwargs):
        self._transform_order = metal_order
        self._assimilation_queue = kwargs.get('assimilate_queue', ForgeQueue.get_for_assimilation_queue(**kwargs))
        self._extracted_data = metal_order.extracted_data
        self._schema_entry = metal_order.schema_entry
        self._source_vertex_data = metal_order.extracted_data['source']
        self._dynamo_driver = DynamoDriver()

    def transform(self):
        source_vertex = PotentialVertex.for_known_vertex(
            self._source_vertex_data, self._schema_entry,
            self._transform_order.identifier_stem, self._transform_order.id_value
        )
        extracted_data = self._extracted_data
        assimilate_orders = [AssimilateObjectOrder.for_source_vertex(source_vertex, extracted_data)]
        arbiter = RuleArbiter(source_vertex, self._schema_entry)
        potentials = arbiter.process_rules(self._extracted_data)
        for potential in potentials:
            potential_vertex = potential[0]
            rule_entry = potential[1]
            assimilate_order = AssimilateObjectOrder(source_vertex, potential_vertex, rule_entry, extracted_data)
            assimilate_orders.append(assimilate_order)
        self._assimilation_queue.add_orders(assimilate_orders)
        self._assimilation_queue.push_orders()
        self._dynamo_driver.mark_object_as_stage_cleared(
            self._transform_order.identifier_stem, self._transform_order.id_value, 'transform')
