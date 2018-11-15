import json
import logging

from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndExtractor
from toll_booth.alg_obj.serializers import AlgEncoder
from toll_booth.alg_tasks.lambda_logging import lambda_logged


@lambda_logged
def handler(event, context):
    logging.info('called the handler for the CredibleWS Extractor, event: %s' % event)
    step_name = event['step_name']
    step_args = event.get('step_args', {})
    if step_name == 'monitor_extraction':
        results = CredibleFrontEndExtractor.get_monitor_extraction(**step_args)
        logging.info('completed a monitor extraction for the CredibleFE Extractor, results: %s' % results)
        return results
    if step_name == 'extraction':
        results = CredibleFrontEndExtractor.extract(**step_args)
        logging.info('completed an extraction for the CredibleFE Extractor, results: %s' % results)
        return json.dumps(results, cls=AlgEncoder)
    if step_name == 'field_value_query':
        results = CredibleFrontEndExtractor.get_field_values(**step_args)
        logging.info('completed a field value query for the CredibleFE Extractor, results: %s' % results)
        return json.dumps(results, cls=AlgEncoder)
    raise NotImplementedError('step named: %s is not registered with the system' % step_name)
