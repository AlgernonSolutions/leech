from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.aws_obj.dynamo_driver import DynamoDriver
from toll_booth.alg_obj.forge.comms.orders import ProcessObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.graph.ogm.generator import VertexCommandGenerator, EdgeCommandGenerator


class ReachTruck:
    def __init__(self, metal_order, **kwargs):
        self._load_order = metal_order
        self._process_queue = kwargs.get('process_queue', ForgeQueue.get_for_process_queue(**kwargs))
        self._ogm = kwargs.get('ogm', DynamoDriver(**kwargs))

    def load(self):
        try:
            self._ogm.write_vertex(self._load_order.vertex)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
        try:
            self._ogm.write_edge(self._load_order.edge)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise e
