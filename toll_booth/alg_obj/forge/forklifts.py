from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.aws_obj.dynamo_driver import DynamoDriver
from toll_booth.alg_obj.forge.comms.orders import ProcessObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.graph.ogm.generator import VertexCommandGenerator, EdgeCommandGenerator
from toll_booth.alg_obj.graph.ogm.ogm import Ogm


class ReachTruck:
    def __init__(self, object_fields, **kwargs):
        self._object_fields = object_fields
        self._ogm = kwargs.get('ogm', Ogm(**kwargs))

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
