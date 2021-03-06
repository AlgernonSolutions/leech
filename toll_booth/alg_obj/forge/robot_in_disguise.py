import logging

from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.sapper.dynamo_driver import LeechDriver
from toll_booth.alg_obj.forge.comms.orders import AssimilateObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.graph.ogm.arbiter import RuleArbiter
from toll_booth.alg_obj.graph.ogm.regulators import VertexRegulator


class DisguisedRobot:
    def __init__(self, metal_order, **kwargs):
        self._transform_order = metal_order
        self._assimilation_queue = kwargs.get('assimilate_queue', ForgeQueue.get_for_assimilation_queue(**kwargs))
        self._extracted_data = metal_order.extracted_data
        self._schema_entry = metal_order.schema_entry
        self._source_vertex_data = metal_order.extracted_data['source']
        self._dynamo_driver = LeechDriver()

    def transform(self):
        regulator = VertexRegulator(self._schema_entry)
        source_vertex = regulator.create_potential_vertex(self._source_vertex_data)
        logging.info('generated source vertex in transform step, source_vertex: %s' % source_vertex.to_json)
        extracted_data = self._extracted_data
        assimilate_orders = []
        arbiter = RuleArbiter(source_vertex, self._schema_entry)
        potentials = arbiter.process_rules(self._extracted_data)
        for potential in potentials:
            potential_vertex = potential[0]
            rule_entry = potential[1]
            assimilate_order = AssimilateObjectOrder(source_vertex, potential_vertex, rule_entry, extracted_data)
            assimilate_orders.append(assimilate_order)
        self._assimilation_queue.add_orders(assimilate_orders)
        self._write_results(source_vertex, potentials)
        self._assimilation_queue.push_orders()

    def _write_results(self, vertex, potentials):
        try:
            self._dynamo_driver.set_transform_results(
                vertex, potentials,
                identifier_stem=vertex.identifier_stem,
                id_value=vertex.id_value)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
