import json
import os

import boto3

from toll_booth.alg_obj.aws.sapper.dynamo_driver import LeechDriver
from toll_booth.alg_obj.aws.sapper.dynamo_scanner import DynamoScanner
from toll_booth.alg_obj.forge.comms.orders import ExtractObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry


class Fixer:
    def __init__(self, **kwargs):
        self._scanner = DynamoScanner(kwargs.get('index_name', 'stalled'))
        self._driver = LeechDriver()
        self._load_graph_orders = []
        self._graph_counter = 0
        self._load_queue_url = os.getenv('LOAD_URL', 'https://sqs.us-east-1.amazonaws.com/803040539655/load')
        self._extract_queue_url = os.getenv('EXTRACT_URL', 'https://sqs.us-east-1.amazonaws.com/803040539655/extract')
        self._load_queue = boto3.resource('sqs').Queue(self._load_queue_url)
        self._extraction_queue = ForgeQueue.get_for_extraction_queue()

    def fix(self):
        stalled_objects = self._scanner.scan_stalled_objects()
        for stalled_object in stalled_objects:
            stalled_stage = stalled_object['last_stage_seen']
            if stalled_stage == 'assimilation':
                self._load(stalled_object)
                continue
            if stalled_stage == 'transformation':
                self._assimilate(stalled_object)
                continue
            if stalled_stage == 'monitoring':
                self._extract(stalled_object)
                continue
            if stalled_stage == 'extraction':
                self._transform(stalled_object)
                continue
            if stalled_stage == 'graphing':
                self._process(stalled_object)
                continue
            raise NotImplementedError(f'stalled stage: {stalled_stage} is not registered with the system')
        self._clean_up()

    def _clean_up(self):
        if self._load_graph_orders:
            self._load_queue.send_messages(
                Entries=self._load_graph_orders
            )
        self._extraction_queue.push_orders()

    def _load(self, stalled_object):
        object_type = stalled_object['object_type']
        if object_type[-1:] == '_':
            return
        stalled_key = {
            'sid_value': {'S': stalled_object['sid_value']},
            'identifier_stem': {'S': stalled_object['identifier_stem']}
        }
        if len(self._load_graph_orders) >= 10:
            self._load_queue.send_messages(
                Entries=self._load_graph_orders
            )
            self._load_graph_orders = []
        self._load_graph_orders.append({
            'Id': str(self._graph_counter),
            'MessageBody': json.dumps({
                'task_name': 'load',
                'task_args': {
                    'keys': stalled_key
                }
            })
        })
        self._graph_counter += 1

    def _assimilate(self, stalled_object):
        disposition = stalled_object['disposition']
        if disposition == 'graphing':
            return self._load(stalled_object)
        raise NotImplementedError()

    def _extract(self, stalled_object):
        identifier_stem = stalled_object['identifier_stem']
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        extractor_names = self._driver.get_extractor_function_names(identifier_stem)
        schema_entry = SchemaVertexEntry.get(stalled_object['object_type'])
        schema_extraction_properties = schema_entry.extract[extractor_names['type']]
        extraction_properties = identifier_stem.for_extractor
        extraction_properties.update(schema_extraction_properties.extraction_properties)
        extractor_name = extractor_names['extraction']
        extraction_order = ExtractObjectOrder(
            identifier_stem, stalled_object['id_value'], extractor_name, extraction_properties, schema_entry)
        self._extraction_queue.add_order(extraction_order)

    def _transform(self, stalled_object):
        pass

    def _process(self, stalled_object):
        pass
