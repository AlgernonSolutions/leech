from collections import OrderedDict

from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver, MissingFieldValuesException
from toll_booth.alg_obj.forge.comms.orders import ExtractObjectOrder
from toll_booth.alg_obj.forge.comms.stage_manager import StageManager
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


class MonitorMouse:
    def __init__(self, monitor_order, **kwargs):
        identifier_stem = IdentifierStem.from_raw(monitor_order.identifier_stem)
        self._identifier_stem = identifier_stem
        self._id_source = monitor_order.id_source
        self._leech_driver = LeechDriver(**kwargs)
        self._local_setup = self._leech_driver.get_field_value_setup(self._identifier_stem)
        self._sample_size = kwargs.get('sample_size', 1000)

    def monitor(self):
        field_identifier_stems = self._derive_value_field_stems()
        remote_value_field_max_min = self._get_remote_value_field_max_mins(field_identifier_stems)
        extraction_orders = []
        for identifier_stem in field_identifier_stems:
            remote_value_max = remote_value_field_max_min[identifier_stem]['max']
            try:
                local_value_field_max = self._get_local_value_field_max(identifier_stem)
            except MissingFieldValuesException:
                local_value_field_max = remote_value_field_max_min[identifier_stem]['min']
            extraction_orders.extend(
                self._generate_extraction_order(identifier_stem, remote_value_max, local_value_field_max))

    def _generate_extraction_order(self, identifier_stem, remote_value_max, local_value_max):
        extraction_orders = []
        if remote_value_max > local_value_max:
            missing_range = range(local_value_max+1, remote_value_max+1)
            missing_range = missing_range[:self._sample_size]
            for entry in missing_range:
                extraction_orders.append(ExtractObjectOrder(
                    identifier_stem, entry,
                ))

    def _get_local_value_field_max(self, identifier_stem):
        return self._leech_driver.query_index_value_max(identifier_stem)

    def _get_remote_value_field_max_mins(self, identifier_stems):
        function_name = self._local_setup['extractor_names']['field_values']
        step_args = {
            'id_source': self._id_source,
            'identifier_stems': identifier_stems
        }
        return StageManager.run_field_value_query(function_name, step_args)

    def _derive_value_field_stems(self):
        stems = []
        paired_identifiers = self._identifier_stem.paired_identifiers
        field_values = self._local_setup['field_values']
        for field_value in field_values:
            field_names = ['id_source', 'id_type', 'id_name']
            named_fields = OrderedDict()
            for field_name in field_names:
                named_fields[field_name] = paired_identifiers[field_name]
            named_fields['data_dict_id'] = field_value
            field_identifier_stem = IdentifierStem('vertex', 'FieldValue', named_fields)
            stems.append(field_identifier_stem)
        return stems
