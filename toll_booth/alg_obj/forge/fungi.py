import json
import logging
import re
import uuid
from collections import OrderedDict
from decimal import Decimal, InvalidOperation

from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.matryoshkas.clerks import ClerkSwarm
from toll_booth.alg_obj.aws.sapper.dynamo_scanner import DynamoScanner
from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver, EmptyIndexException
from toll_booth.alg_obj.forge.comms.orders import TransformObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.forge.comms.stage_manager import StageManager
from toll_booth.alg_obj.forge.credible_specifics.change_types import ChangeTypes
from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
from toll_booth.alg_obj.forge.extractors.credible_fe.mule_team import CredibleMuleTeam
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem, VertexRegulator
from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaVertexEntry
from toll_booth.alg_obj.serializers import AlgEncoder
from toll_booth.alg_tasks.task_obj import metered, InsufficientOperationTimeException


class Spore:
    def __init__(self, identifier_stem, driving_identifier_stem, **kwargs):
        identifier_stem = IdentifierStem.from_raw(identifier_stem)
        driving_identifier_stem = IdentifierStem.from_raw(driving_identifier_stem)
        self._spore_id = uuid.uuid4().hex
        self._identifier_stem = identifier_stem
        self._driving_identifier_stem = driving_identifier_stem
        self._leech_driver = LeechDriver(table_name='VdGraphObjects')
        self._extractor_setup = self._leech_driver.get_extractor_setup(driving_identifier_stem)
        self._schema_entry = SchemaVertexEntry.retrieve(driving_identifier_stem.object_type)
        self._sample_size = kwargs.get('sample_size', 1000)
        self._extraction_profile = self._generate_extraction_profile()
        self._driving_vertex_regulator = VertexRegulator.get_for_object_type(driving_identifier_stem.object_type)

    def propagate(self):
        remote_id_values = self._perform_remote_monitor_extraction()
        local_id_values = self._perform_local_monitor_extraction()
        self._manage_driven_ids(remote_id_values, local_id_values)
        self._mark_propagation(remote_id_values)
        return {'propagation_id': self._spore_id, 'id_source': self._driving_identifier_stem.get('id_source')}

    def _perform_remote_monitor_extraction(self):
        step_args = self._extractor_setup.copy()
        step_args.update(self._driving_identifier_stem.for_extractor)
        step_args.update(self._schema_entry.extract[self._extractor_setup['type']].extraction_properties)
        manager_args = (self._extractor_setup['monitor_extraction'], step_args)
        remote_id_values = StageManager.run_monitoring_extraction(*manager_args)
        return set(remote_id_values)

    def _perform_local_monitor_extraction(self):
        identifier_stem = self._driving_identifier_stem
        vertex_regulator = self._driving_vertex_regulator
        local_id_values = self._leech_driver.get_local_id_values(identifier_stem, vertex_regulator=vertex_regulator)
        return local_id_values

    def _generate_extraction_profile(self):
        extraction_properties = self._driving_identifier_stem.for_extractor
        schema_extraction_properties = self._schema_entry.extract[self._extractor_setup['type']]
        extraction_properties.update(schema_extraction_properties.extraction_properties)
        return extraction_properties

    def _mark_propagation(self, remote_id_values, **kwargs):
        change_types = ChangeTypes.get(leech_driver=self._leech_driver)
        kwargs['change_types'] = change_types
        self._leech_driver.mark_propagated_vertexes(
            self._spore_id, self._identifier_stem, self._driving_identifier_stem, remote_id_values, **kwargs
        )

    def _manage_driven_ids(self, remote_id_values, local_id_values):
        local_linked_values = local_id_values['linked']
        newly_linked_id_values = remote_id_values - local_linked_values
        unlinked_id_values = local_linked_values - remote_id_values
        new_id_values = remote_id_values - local_id_values['all']
        self._put_new_ids(new_id_values)
        self._unlink_old_ids(unlinked_id_values)
        self._link_new_ids(newly_linked_id_values)

    def _unlink_old_ids(self, id_values):
        for id_value in id_values:
            object_data = self._identifier_stem.for_extractor
            object_data['id_value'] = id_value
            potential_vertex = self._driving_vertex_regulator.create_potential_vertex(object_data)
            try:
                self._leech_driver.set_link_object(
                    potential_vertex.internal_id, self._identifier_stem.get('id_source'), True,
                    identifier_stem=self._identifier_stem, id_value=id_value
                )
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise e

    def _link_new_ids(self, id_values):
        for id_value in id_values:
            object_data = self._identifier_stem.for_extractor
            object_data['id_value'] = id_value
            potential_vertex = self._driving_vertex_regulator.create_potential_vertex(object_data)
            try:
                self._leech_driver.set_link_object(
                    potential_vertex.internal_id, self._identifier_stem.get('id_source'), False,
                    identifier_stem=self._identifier_stem, id_value=id_value
                )
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise e

    def _put_new_ids(self, id_values):
        for id_value in id_values:
            object_data = self._identifier_stem.for_extractor
            object_data['id_value'] = id_value
            potential_vertex = self._driving_vertex_regulator.create_potential_vertex(object_data)
            try:
                self._leech_driver.set_assimilated_vertex(
                    potential_vertex, False, identifier_stem=self._identifier_stem, id_value=id_value)
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise e


class Mycelium:
    def __init__(self, propagation_id, id_source, **kwargs):
        self._propagation_id = propagation_id
        self._id_source = id_source
        self._leech_driver = LeechDriver(table_name='VdGraphObjects')
        self._driving_identifier_stem = None
        self._extracted_identifier_stem = None
        self._propagation_identifier_stem = None
        self._context = kwargs['context']
        self._change_types = ChangeTypes.get(leech_driver=self._leech_driver)

    def creep(self):
        with CredibleFrontEndDriver(self._id_source) as driver:
            for entry in self._leech_driver.get_propagated_vertexes(self._propagation_id):
                if not self._driving_identifier_stem:
                    self._driving_identifier_stem = IdentifierStem.from_raw(entry['driving_identifier_stem'])
                if not self._extracted_identifier_stem:
                    self._extracted_identifier_stem = IdentifierStem.from_raw(entry['extracted_identifier_stem'])
                if not self._propagation_identifier_stem:
                    self._propagation_identifier_stem = IdentifierStem.from_raw(entry['identifier_stem'])
                try:
                    self._creep(
                        entry,
                        identifier_stem=self._extracted_identifier_stem,
                        driving_identifier_stem=self._driving_identifier_stem,
                        context=self._context,
                        driver=driver
                    )
                except InsufficientOperationTimeException:
                    return False
        return {'propagation_id': self._propagation_id, 'id_source': self._id_source}

    @metered
    def _creep(self, entry, **kwargs):
        driving_identifier_stem = kwargs['driving_identifier_stem']
        driver = kwargs['driver']
        identifier_stem = IdentifierStem.from_raw(entry['identifier_stem'])
        id_value = entry['driving_id_value']
        category = identifier_stem.get('category')
        change_category = self._change_types.get_category_by_name(category)
        logging.info(
            f'started the extraction for id_value: {id_value}, change_category_id: {change_category.category_id}')
        local_max_value = self._get_local_max_value(id_value, change_category)
        extraction_args = {
            'driving_id_type': driving_identifier_stem.get('id_type'),
            'driving_id_name': driving_identifier_stem.get('id_name'),
            'driving_id_value': id_value,
            'local_max_value': local_max_value,
            'category_id': change_category.category_id,
            'driving_identifier_stem': driving_identifier_stem,
            'identifier_stem': identifier_stem,
            'category': change_category
        }
        remote_changes = driver.get_change_logs(**extraction_args)
        logging.info(
            f'completed the extraction for id_value: {id_value}, change_category_id: {change_category.category_id}')
        self._mark_creep_vertexes(remote_changes, **extraction_args)

    def _get_local_max_value(self, driving_id_value, change_type):
        id_source = self._driving_identifier_stem.get('id_source')
        id_type = self._driving_identifier_stem.get('id_type')
        change_stem = f'#{id_source}#{id_type}#{driving_id_value}#{change_type}'
        try:
            local_max_value = self._leech_driver.scan_index_value_max(change_stem)
        except EmptyIndexException:
            local_max_value = None
        return local_max_value

    def _mark_creep_vertexes(self, remote_changes, category, **kwargs):
        clerks = ClerkSwarm(self._leech_driver.table_name, 'mark_creep')
        driving_id_value = kwargs['driving_id_value']
        propagation_identifier_stem = kwargs['identifier_stem']
        for remote_change in remote_changes:
            action = remote_change['Action']
            action_id = category.get_action_id(action)
            change_date_utc = remote_change['UTCDate']
            record = remote_change['Record']
            pairs = OrderedDict()
            pairs['category'] = str(category)
            pairs['id_value'] = driving_id_value
            pairs['action'] = str(action)
            pairs['record'] = record
            pairs['done_by'] = remote_change['Done By']
            pairs['change_date_utc'] = str(change_date_utc.timestamp())
            pairs.update(self._driving_identifier_stem.paired_identifiers)
            pending_write = kwargs.copy()
            pending_write['identifier_stem'] = str(IdentifierStem('creep', 'ChangeLog', pairs))
            pending_write['sid_value'] = self._propagation_id
            pending_write['propagation_id'] = self._propagation_id
            pending_write['propagation_identifier_stem'] = str(propagation_identifier_stem)
            pending_write['creep_identifier'] = f'#{str(driving_id_value)}#{str(category)}#{str(action)}#'
            pending_write['remote_change'] = json.dumps(remote_change, cls=AlgEncoder)
            pending_write['driving_identifier_stem'] = str(self._driving_identifier_stem)
            pending_write['extracted_identifier_stem'] = str(self._extracted_identifier_stem)
            pending_write['category'] = str(category)
            pending_write['action'] = str(action)
            pending_write['action_id'] = action_id
            try:
                clerks.add_pending_write(pending_write)
            except RuntimeError:
                pass
        clerks.send()
        self._leech_driver.delete_propagated_vertex(self._propagation_id, self._propagation_identifier_stem)


class Mushroom:
    def __init__(self, propagation_id, id_source, **kwargs):
        self._propagation_id = propagation_id
        self._id_source = id_source
        self._leech_driver = LeechDriver(table_name='VdGraphObjects')
        self._leech_scanner = DynamoScanner(table_name='VdGraphObjects')
        self._transform_queue = kwargs.get(
            'transform_queue', ForgeQueue.get_for_transform_queue(swarm=False, auto_send_threshold=1, **kwargs))
        self._change_types = ChangeTypes.get(leech_driver=self._leech_driver)
        self._driving_identifier_stem = None
        self._extracted_identifier_stem = None
        self._schema_entry = None
        self._mapping = None
        self._local_max_value = None
        self._enriched_data = None
        self._context = kwargs['context']
        self._results = {}
        self._checked_emp_ids = {}

    def fruit(self):
        cascade_args = {'propagation_id': self._propagation_id}
        logging.info('beginning a vertex driven extraction for propagation_id: %s' % self._propagation_id)
        for id_value in self._leech_driver.get_creep_id_values(**cascade_args):
            logging.info('beginning the extraction for id_value: %s' % id_value)
            cascade_args['id_value'] = id_value
            for change_category in self._leech_driver.get_creep_categories(change_types=self._change_types,
                                                                           **cascade_args):
                logging.info(
                    'beginning the extraction for id_value: %s, category: %s' % (id_value, str(change_category)))
                cascade_args['change_category'] = change_category
                for change_type in self._leech_driver.get_creep_actions(**cascade_args):
                    logging.info('beginning the extraction for id_value: %s, category: %s, action: %s' %
                                 (id_value, str(change_category), str(change_type)))
                    cascade_args['change_action'] = change_type
                    for entry in self._leech_driver.get_creep_changes(**cascade_args):
                        logging.debug(
                            'beginning the extraction for id_value: %s, category: %s, action: %s, entry: %s'
                            % (id_value, str(change_category), str(change_type), str(entry)))
                        self._populate_common_fields(entry)
                        self._check_enriched_data(id_value, change_category, change_type)
                        try:
                            fruit = self._fruit(
                                entry['remote_change'],
                                id_value=id_value,
                                change_category=change_category,
                                change_type=change_type,
                                context=self._context
                            )
                            self._store_fruit(change_category, change_type, fruit)
                            logging.debug('completed the extraction for id_value: %s, '
                                          'category: %s, action: %s, entry: %s'
                                          % (id_value, str(change_category), str(change_type), str(entry)))
                            self._set_fruit(fruit, entry['identifier_stem'])
                        except InsufficientOperationTimeException:
                            self._transform_queue.push_orders()
                            return False
                    logging.info('completed the extraction for id_value: %s, category: %s, action: %s' %
                                 (id_value, str(change_category), str(change_type)))
                logging.info(
                    'completed the extraction for id_value: %s, category: %s' % (id_value, str(change_category)))
            logging.info('completed the extraction for id_value: %s' % id_value)
        logging.info('completed a vertex driven extraction for propagation_id: %s' % self._propagation_id)
        return True

    @metered
    def _fruit(self, remote_change, **kwargs):
        change_log_data = self._generate_change_log(remote_change, **kwargs)
        return change_log_data

    def _generate_change_log(self, remote_change, **kwargs):
        change_category = kwargs['change_category']
        change_type = kwargs['change_type']
        change_date_utc = remote_change['UTCDate']
        extracted_data = self._build_change_log_extracted_data(remote_change, self._mapping)
        id_source = self._driving_identifier_stem.get('id_source')
        source_data = {
            'change_date_utc': extracted_data['change_date_utc'],
            'change_description': extracted_data['change_description'],
            'change_date': extracted_data['change_date'],
            'action': extracted_data['action'],
            'action_id': change_category.get_action_id(extracted_data['action']),
            'id_source': id_source,
            'id_type': 'ChangeLog',
            'id_name': 'change_date_utc',
            'by_emp_id': self._enriched_data.get_by_emp_id(change_date_utc)
        }
        returned_data = {
            'source': source_data
        }
        changed_target = self._build_changed_targets(id_source, extracted_data, change_type)
        if changed_target:
            returned_data['changed_target'] = changed_target
        change_target = self._enriched_data.get_change_target(change_date_utc)
        if change_target is not None:
            returned_data['change_target'] = change_target
        return returned_data

    def _set_fruit(self, change_object, creep_identifier_stem):
        id_value = change_object['source']['change_date_utc']
        identifier_stem = self._calculate_change_log_identifier_stem(change_object)
        mark_args = (id_value, creep_identifier_stem, identifier_stem, change_object)
        is_working_already = self._mark_objects_as_working(*mark_args)
        if not is_working_already:
            order_args = (identifier_stem, id_value, change_object, self._schema_entry)
            transform_order = TransformObjectOrder(*order_args)
            self._transform_queue.add_order(transform_order)

    def _mark_objects_as_working(self, id_value, creep_identifier_stem, identifier_stem, extracted_data):
        put_args = (self._propagation_id, creep_identifier_stem, extracted_data)
        put_kwargs = {
            'identifier_stem': identifier_stem, 'id_value': id_value
        }
        return self._leech_driver.mark_fruited_vertex(*put_args, **put_kwargs)

    @classmethod
    def _calculate_change_log_identifier_stem(cls, extracted_data):
        pairs = {
            'id_source': extracted_data['source']['id_source'],
            'id_type': extracted_data['source']['id_type'],
            'id_name': extracted_data['source']['id_name']
        }
        identifier_stem = IdentifierStem('vertex', 'ChangeLog', pairs)
        return identifier_stem

    def perform_enrichment(self, driving_id_value, change_category, change_action, **kwargs):
        mule_team = CredibleMuleTeam(self._id_source)
        enrichment_args = {
            'driving_id_type': self._driving_identifier_stem.get('id_type'),
            'driving_id_name': self._driving_identifier_stem.get('id_name'),
            'driving_id_value': driving_id_value,
            'local_max_value': self._local_max_value,
            'category_id': change_category.category_id,
            'action_id': int(change_action.action_id),
            'get_details': kwargs['get_details'],
            'get_emp_ids': kwargs['get_emp_ids'],
            'checked_emp_ids': self._checked_emp_ids
        }
        enriched_data = mule_team.enrich_data(**enrichment_args)
        return enriched_data

    @classmethod
    def _split_record_id(cls, field_value, **kwargs):
        record_id, record_type = cls._split_entry(field_value)
        return {'record_id': record_id, 'record_type': record_type}

    @classmethod
    def _split_entry(cls, field_value):
        non_numeric_inside = re.compile('(?P<outside>[\w\s]+?)\s*\((?P<inside>(?=[a-zA-Z\s])[\w\s\d]+)\)')
        numeric_inside = re.compile('(?P<outside>[\w\s]+?)\s*\((?P<inside>[\d]+)\)')
        no_parenthesis_number = re.compile('^((?![()])\d)*$')
        has_numeric_inside = numeric_inside.search(field_value)
        has_non_numeric_inside = non_numeric_inside.search(field_value)
        is_just_number = no_parenthesis_number.search(field_value) is not None
        if has_numeric_inside:
            id_type = has_numeric_inside.group('outside')
            id_value = has_numeric_inside.group('inside')
            return Decimal(id_value), id_type
        if has_non_numeric_inside:
            id_value = has_non_numeric_inside.group('outside')
            id_type = has_non_numeric_inside.group('inside')
            try:
                return Decimal(id_value), id_type
            except InvalidOperation:
                return id_type, id_value
        if is_just_number:
            return Decimal(field_value), None
        return field_value, None

    @classmethod
    def _convert_datetime_utc(cls, field_value):
        from toll_booth.alg_obj.utils import convert_credible_fe_datetime_to_python
        return convert_credible_fe_datetime_to_python(field_value, True)

    @classmethod
    def _convert_datetime(cls, field_value):
        from toll_booth.alg_obj.utils import convert_credible_fe_datetime_to_python
        return convert_credible_fe_datetime_to_python(field_value, False)

    def _generate_mapping(self):
        for extractor in self._schema_entry.extract.values():
            extraction_properties = extractor.extraction_properties
            mapping = extraction_properties['mapping']
            id_source_mapping = mapping.get(self._id_source, mapping['default'])
            object_mapping = id_source_mapping[self._driving_identifier_stem.get('id_type')]
            return object_mapping

    def _populate_common_fields(self, entry):
        if not self._extracted_identifier_stem:
            self._extracted_identifier_stem = IdentifierStem.from_raw(entry['extracted_identifier_stem'])
        if not self._driving_identifier_stem:
            self._driving_identifier_stem = IdentifierStem.from_raw(entry['driving_identifier_stem'])
        if not self._schema_entry:
            self._schema_entry = SchemaVertexEntry.retrieve(self._extracted_identifier_stem.object_type)
        if not self._mapping:
            self._mapping = self._generate_mapping()
        if not self._local_max_value:
            self._local_max_value = entry['local_max_value']
        return

    def _check_enriched_data(self, id_value, change_category, change_type):
        if self._enriched_data is None:
            enriched_data = EnrichedData(self, id_value, change_category, change_type)
            enriched_data.update_enriched_data(id_value, change_category, change_type)
            self._enriched_data = enriched_data
        if not self._enriched_data.is_current(id_value, change_category, change_type):
            self._enriched_data.update_enriched_data(
                id_value, change_category, change_type)

    def _store_fruit(self, change_category, change_type, fruit):
        if str(change_category) not in self._results:
            self._results[str(change_category)] = {}
        if str(change_type) not in self._results[str(change_category)]:
            self._results[str(change_category)][str(change_type)] = []
        self._results[str(change_category)][str(change_type)].append(fruit)

    @classmethod
    def _build_changed_targets(cls, id_source, extracted_data, change_type):
        changed_target = []
        client_id = extracted_data.get('client_id', None)
        clientvisit_id = extracted_data.get('clientvisit_id', None)
        if client_id and client_id != 0:
            changed_target.append({
                'id_source': id_source,
                'id_type': 'Clients',
                'id_name': 'client_id',
                'id_value': Decimal(client_id)
            })
        if clientvisit_id and clientvisit_id != '0':
            changed_target.append({
                'id_source': id_source,
                'id_type': 'ClientVisit',
                'id_name': 'clientvisit_id',
                'id_value': Decimal(clientvisit_id)
            })
        if extracted_data.get('record', None):
            record = extracted_data.get('record')
            id_type = record['record_type']
            if id_type is None:
                id_type = change_type.id_type
            if id_type not in ['ClientVisit', 'Clients', 'unspecified']:
                id_name = change_type.id_name
                changed_target.append({
                    'id_source': id_source,
                    'id_type': id_type,
                    'id_name': id_name,
                    'id_value': Decimal(record['record_id'])
                })
            if id_type == 'unspecified':
                static_values = change_type.change_target
                if static_values == 'dynamic':
                    raise RuntimeError('could not determine the id_type and id_name for: %s' % extracted_data)
                static_values['id_value'] = Decimal(record['record_id'])
                static_values['id_source'] = id_source
                changed_target.append(static_values)
        return changed_target

    def _build_change_log_extracted_data(self, remote_change, mapping):
        extracted_data = {}
        for field_name, field_value in remote_change.items():
            if field_name in mapping:
                row_mapping = mapping[field_name]
                field_name = row_mapping['name']
                mutation = row_mapping['mutation']
                if mutation and field_value:
                    field_value = getattr(self, '_' + mutation)(field_value)
                extracted_data[field_name] = field_value
        return extracted_data


class EnrichedData:
    def __init__(self, host_shroom, id_value, change_category, change_action):
        self._host_shroom = host_shroom
        self._id_value = id_value
        self._change_category = change_category
        self._change_action = change_action
        self._enriched_data = None

    @property
    def enriched_data(self):
        return self._enriched_data

    @property
    def is_static(self):
        return self._change_action.is_static

    def is_current(self, id_value, change_category, change_action):
        if self._enriched_data is None:
            return False
        return id_value == self._id_value and change_category == self._change_category and change_action == self._change_action

    def update_enriched_data(self, id_value, change_category, change_action):
        self._id_value = id_value
        self._change_category = change_category
        self._change_action = change_action
        enrichment_args = {
            'get_emp_ids': self.is_static is False,
            'get_details': change_action.has_details is True
        }
        if enrichment_args['get_emp_ids'] or enrichment_args['get_details']:
            enriched_data = self._host_shroom.perform_enrichment(id_value, change_category, change_action,
                                                                 **enrichment_args)
            self._enriched_data = enriched_data
            return
        self._enriched_data = {}

    def get_by_emp_id(self, change_date_utc):
        if self.is_static:
            return int(self._id_value)
        timestamp = change_date_utc.timestamp()
        emp_id = self._enriched_data['emp_ids'][timestamp]
        if emp_id is None:
            logging.error(
                'the data from the remote source was missing a '
                'required by_emp_id field at change_date: %s, setting to default value 0' % str(change_date_utc)
            )
            return 0
        return int(emp_id)

    def get_change_target(self, change_date_utc):
        if self._change_action.has_details:
            return self._enriched_data['change_detail'][change_date_utc.timestamp()]
        return None
