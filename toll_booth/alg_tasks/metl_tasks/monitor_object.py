import logging
from collections import OrderedDict

from aws_xray_sdk.core import xray_recorder

from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
from toll_booth.alg_obj.forge.lizards import MonitorLizard
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem
from toll_booth.alg_tasks.task_obj import remote_task


@remote_task
@xray_recorder.capture('monitor')
def monitor_object(*args, **kwargs):
    logging.info('starting a monitor object task with args/kwargs: %s/%s' % (args, kwargs))
    task_args = kwargs['task_args']
    identifier_stem = task_args['metal_order'].identifier_stem
    identifier_stem = IdentifierStem.from_raw(identifier_stem)
    paired_identifiers = identifier_stem.paired_identifiers
    driver = LeechDriver(**kwargs)
    field_values = driver.get_field_values(identifier_stem)
    results = []
    for field_value in field_values:
        field_names = ['id_source', 'id_type', 'id_name']
        named_fields = OrderedDict()
        for field_name in field_names:
            named_fields[field_name] = paired_identifiers[field_name]
        named_fields['data_dict_id'] = field_value
        field_identifier_stem = IdentifierStem('vertex', 'FieldValue', named_fields)
        logging.info('starting to build the lizard, with identifier stem: %s' % str(field_identifier_stem))
        lizard = MonitorLizard(identifier_stem=field_identifier_stem)
        logging.info('built the lizard')
        invocation_results = lizard.monitor()
        logging.info('ran the lizard with results: %s' % invocation_results)
        results.append(invocation_results)
    logging.info('finished a monitor object task with results: %s' % results)
    return results
