import logging

from toll_booth.alg_obj.forge.extractors.credible_ws import CredibleWebServiceExtractor

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)
logging.basicConfig(format='[%(levelname)s] %(asctime)s %(message)s', level=logging.INFO)
logging.getLogger('aws_xray_sdk').setLevel(logging.INFO)


def handler(event, context):
    logging.info('called the handler for the CredibleWS Extractor, event: %s' % event)
    step_name = event['step_name']
    step_args = event.get('step_args', {})
    if step_name == 'index_query':
        results = CredibleWebServiceExtractor.get_current_remote_max_min_id(**step_args)
        logging.info('completed an index query for the CredibleWS Extractor, results: %s' % results)
        return results
    if step_name == 'extraction':
        results = CredibleWebServiceExtractor.extract(**step_args)
        logging.info('completed an extraction for the CredibleWS Extractor, results: %s' % results)
        return results
    raise NotImplementedError('step named: %s is not registered with the system' % step_name)
