from collections import OrderedDict

from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver, EmptyIndexException
from toll_booth.alg_obj.forge.comms.orders import ExtractObjectOrder, LinkObjectOrder, TransformObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.forge.comms.stage_manager import StageManager
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem, VertexRegulator
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry


class Spore:
    def __init__(self, identifier_stem, driving_identifier_stem, **kwargs):
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        self._identifier_stem = identifier_stem
        self._driving_identifier_stem = driving_identifier_stem
        self._leech_driver = LeechDriver(table_name='VdGraphObjects')
        self._extractor_setup = self._leech_driver.get_extractor_setup(identifier_stem, include_field_values=False)
        self._driving_extractor_setup = self._leech_driver.get_extractor_setup(driving_identifier_stem)
        self._schema_entry = SchemaVertexEntry.get(identifier_stem.object_type)
        self._transform_queue = kwargs.get('transform_queue', ForgeQueue.get_for_transform_queue(swarm=True, **kwargs))
        self._link_queue = kwargs.get('link_queue', ForgeQueue.get_for_link_queue(swarm=True, **kwargs))
        self._sample_size = kwargs.get('sample_size', 1000)
        self._extraction_profile = self._generate_extraction_profile()

    def propagate(self):
        remote_id_values = self._perform_remote_monitor_extraction()
        local_id_values = self._perform_local_monitor_extraction()
        newly_linked_id_values = remote_id_values - local_id_values
        self._put_new_ids(self._driving_identifier_stem, newly_linked_id_values)
        unlinked_id_values = local_id_values - remote_id_values
        self._send_link_orders(newly_linked_id_values, unlinked_id_values)
        identifier_stems = []
        for id_value in remote_id_values:
            specified_stem = self._identifier_stem.specify(id_value)
            try:
                local_max_value = self._leech_driver.query_index_value_max(specified_stem)
            except EmptyIndexException:
                local_max_value = None
            identifier_stems.append({'identifier_stem': specified_stem, 'id_value': local_max_value})
        remote_objects = self._perform_remote_extraction(identifier_stems)
        for remote_object in remote_objects:
            for identifier, extracted_data in remote_object.items():
                id_value = identifier[1]
                identifier_stem = identifier[0]
                is_working_already = self._mark_objects_as_working(id_value, identifier_stem, remote_object)
                if not is_working_already:
                    order_args = (identifier_stem, id_value, remote_object, self._schema_entry)
                    transform_order = TransformObjectOrder(*order_args)
                    self._transform_queue.add_order(transform_order)
        self._transform_queue.push_orders()

    def _get_remote_field_values(self, identifier_stems):
        function_name = self._extractor_setup['extractor_names']['field_values']
        step_args = {
            'identifier_stems': identifier_stems
        }
        return StageManager.run_field_value_query(function_name, step_args)

    def _get_local_field_value_keys(self, identifier_stem):
        return self._leech_driver.get_local_field_value_keys(identifier_stem)

    def _get_max_local_field_value_key(self, identifier_stem):
        return self._leech_driver.get_local_field_value_keys_max(identifier_stem)

    def _send_link_orders(self, newly_linked_id_values, unlinked_id_values):
        for id_value in unlinked_id_values:
            unlink_order = self._generate_link_order(id_value, unlink=True)
            self._link_queue.add_order(unlink_order)
        for id_value in newly_linked_id_values:
            link_order = self._generate_link_order(id_value, unlink=False)
            self._link_queue.add_order(link_order)
        self._link_queue.push_orders()

    def _perform_remote_extraction(self, identifier_stems):
        step_args = {
            'id_source': self._identifier_stem.get('id_source'),
            'identifiers': identifier_stems
        }
        extraction_profile = self._schema_entry.extract[self._extractor_setup['type']]
        step_args.update(extraction_profile.extraction_properties)
        step_args.update(self._identifier_stem.for_extractor)
        manager_args = (self._extractor_setup['monitor_extraction'], step_args)
        remote_objects = StageManager.run_extraction(*manager_args)
        return remote_objects

    def _perform_remote_monitor_extraction(self):
        step_args = self._driving_extractor_setup.copy()
        step_args.update(self._driving_identifier_stem.for_extractor)
        step_args.update(self._schema_entry.extract[self._extractor_setup['type']].extraction_properties)
        manager_args = (self._extractor_setup['monitor_extraction'], step_args)
        remote_id_values = StageManager.run_monitoring_extraction(*manager_args)
        return set(remote_id_values)

    def _perform_local_monitor_extraction(self):
        local_id_values = self._leech_driver.get_local_id_values(self._identifier_stem)
        if not local_id_values:
            return set()
        return set(local_id_values)

    def _generate_extraction_profile(self):
        extraction_properties = self._identifier_stem.for_extractor
        schema_extraction_properties = self._schema_entry.extract[self._extractor_setup['type']]
        extraction_properties.update(schema_extraction_properties.extraction_properties)
        if not extraction_properties['driving_vertex_type']:
            raise RuntimeError('attempted to perform a propagation task on object: %s, this object is '
                               'not able to be propagated' % str(self._identifier_stem))
        return extraction_properties

    def _generate_extraction_order(self, id_value):
        extractor_name = self._extractor_setup['extraction']
        extraction_profile = self._extraction_profile
        return ExtractObjectOrder(
            self._identifier_stem, id_value, extractor_name, extraction_profile, self._schema_entry)

    def _generate_link_order(self, id_value, unlink):
        return LinkObjectOrder(
            self._identifier_stem, id_value, unlink
        )

    def _derive_value_field_stems(self, id_value):
        stems = []
        paired_identifiers = self._identifier_stem.paired_identifiers.copy()
        paired_identifiers['id_value'] = id_value
        field_values = self._extractor_setup['field_values']
        for field_value in field_values:
            field_names = ['id_source', 'id_type', 'id_name']
            named_fields = OrderedDict()
            for field_name in field_names:
                named_fields[field_name] = paired_identifiers[field_name]
            named_fields['id_value'] = id_value
            named_fields['data_dict_id'] = field_value
            field_identifier_stem = IdentifierStem('vertex', 'FieldValue', named_fields)
            stems.append(field_identifier_stem)
        return stems

    def _mark_objects_as_working(self, id_value, identifier_stem, extracted_data):
            return self._leech_driver.put_vertex_driven_seed(
                extracted_data, identifier_stem=identifier_stem, id_value=id_value)

    def _put_new_ids(self, identifier_stem, id_values):
        vertex_regulator = VertexRegulator.get_for_object_type(identifier_stem.object_type)
        for id_value in id_values:
            object_data = identifier_stem.for_extractor
            object_data['id_value'] = id_value
            potential_vertex = vertex_regulator.create_potential_vertex(object_data)
            try:
                self._leech_driver.set_assimilated_vertex(
                    potential_vertex, False, identifier_stem=identifier_stem, id_value=id_value)
                self._leech_driver.set_link_object(
                    potential_vertex.internal_id, self._identifier_stem.get('id_source'), False,
                    identifier_stem=identifier_stem, id_value=id_value
                )
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise e
