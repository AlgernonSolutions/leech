import json
import logging
import re
import uuid
from collections import OrderedDict
from decimal import Decimal, InvalidOperation

from botocore.exceptions import ClientError

from toll_booth.alg_obj.aws.matryoshkas.clerks import ClerkSwarm
from toll_booth.alg_obj.aws.sapper.dynamo_scanner import DynamoScanner
from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver, EmptyIndexException, MissingObjectException
from toll_booth.alg_obj.forge.comms.orders import TransformObjectOrder
from toll_booth.alg_obj.forge.comms.queues import ForgeQueue
from toll_booth.alg_obj.forge.comms.stage_manager import StageManager
from toll_booth.alg_obj.forge.credible_specifics import ChangeTypes, ChangeTypeCategory
from toll_booth.alg_obj.forge.extractors.credible_fe import CredibleFrontEndDriver
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
        self._schema_entry = SchemaVertexEntry.get(driving_identifier_stem.object_type)
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
        self._context = kwargs['context']

    def creep(self):
        with CredibleFrontEndDriver(self._id_source) as driver:
            for entry in self._leech_driver.get_propagated_vertexes(self._propagation_id):
                if not self._driving_identifier_stem:
                    self._driving_identifier_stem = IdentifierStem.from_raw(entry['driving_identifier_stem'])
                if not self._extracted_identifier_stem:
                    self._extracted_identifier_stem = IdentifierStem.from_raw(entry['extracted_identifier_stem'])
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
        change_category_data = self._leech_driver.get_changelog_types(category=identifier_stem.get('category'))
        change_category = ChangeTypeCategory.get_from_change_identifiers(change_category_data)
        logging.info(f'started the extraction for id_value: {id_value}, change_category_id: {change_category.category_id}')
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
        logging.info(f'completed the extraction for id_value: {id_value}, change_category_id: {change_category.category_id}')
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
            kwargs['identifier_stem'] = str(IdentifierStem('creep', 'ChangeLog', pairs))
            kwargs['sid_value'] = self._propagation_id
            kwargs['propagation_id'] = self._propagation_id
            kwargs['propagation_identifier_stem'] = str(propagation_identifier_stem)
            kwargs['creep_identifier'] = f'#{str(driving_id_value)}#{str(category)}#{str(action)}#'
            kwargs['remote_change'] = json.dumps(remote_change, cls=AlgEncoder)
            kwargs['driving_identifier_stem'] = str(self._driving_identifier_stem)
            kwargs['extracted_identifier_stem'] = str(self._extracted_identifier_stem)
            try:
                clerks.add_pending_write(kwargs)
            except RuntimeError:
                pass
        clerks.send()


class Mushroom:
    def __init__(self, propagation_id, id_source, **kwargs):
        self._propagation_id = propagation_id
        self._id_source = id_source
        self._leech_driver = LeechDriver(table_name='VdGraphObjects')
        self._leech_scanner = DynamoScanner(table_name='VdGraphObjects')
        self._transform_queue = kwargs.get('transform_queue', ForgeQueue.get_for_transform_queue(swarm=True, **kwargs))
        self._change_types = ChangeTypes.get(leech_driver=self._leech_driver)
        self._driving_identifier_stem = None
        self._extracted_identifier_stem = None
        self._schema_entry = None
        self._context = kwargs['context']

    def fruit(self):
        mapping = self._generate_mapping()
        with CredibleFrontEndDriver(self._id_source) as driver:
                for id_value in creep_id_values:

                    for entry in self._leech_driver.get_creep_vertexes(self._propagation_id, change_category, id_value):
                        if not self._extracted_identifier_stem:
                            self._extracted_identifier_stem = IdentifierStem.from_raw(entry['extracted_identifier_stem'])
                        if not self._driving_identifier_stem:
                            self._driving_identifier_stem = IdentifierStem.from_raw(entry['driving_identifier_stem'])
                        if not self._schema_entry:
                            self._schema_entry = SchemaVertexEntry.get(self._extracted_identifier_stem.object_type)
                        try:
                            self._fruit(change_category)
                        except InsufficientOperationTimeException:
                            return False
        return True

    @metered
    def _fruit(self, remote_change, local_max_value, id_value, change_category, mapping, driver):
        change_detail_data = {}
        change_log_data = self._generate_change_log(remote_change, change_category, mapping)
        action_id = change_log_data['source']['action_id']
        change_log_data['change_target'] = []
        if change_category.action_has_details(action_id):
            try:
                change_details = change_detail_data[action_id]
            except KeyError:
                change_details = self._perform_change_detail_extraction(
                    driver, id_value, local_max_value, change_category, action_id, self._driving_identifier_stem
                )
                change_detail_data[action_id] = change_details
            if change_details:
                change_log_data['change_target'] = change_details[change_date]
        fruit.append(change_log_data)
        logging.info(
            f'finished the extraction for id_value: {id_value}, change_category_id: {change_category.category_id}')

    def _generate_change_log(self, remote_change, change_category, mapping):
        extracted_data = {}
        changed_target = []
        id_source = self._driving_identifier_stem.get('id_source')
        for field_name, field_value in remote_change.items():
            if field_name in mapping:
                row_mapping = mapping[field_name]
                field_name = row_mapping['name']
                mutation = row_mapping['mutation']
                if mutation and field_value:
                    field_value = getattr(self, '_' + mutation)(field_value)
                extracted_data[field_name] = field_value
        if extracted_data.get('client_id', None):
            changed_target.append({
                'id_source': id_source,
                'id_type': 'Clients',
                'id_name': 'client_id',
                'id_value': Decimal(extracted_data.get('client_id'))
            })
        if extracted_data.get('clientvisit_id', None):
            changed_target.append({
                'id_source': id_source,
                'id_type': 'ClientVisit',
                'id_name': 'clientvisit_id',
                'id_value': Decimal(extracted_data.get('clientvisit_id'))
            })
        if extracted_data.get('record', None):
            record = extracted_data.get('record')
            id_type = record['record_type']
            if id_type not in ['ClientVisit', 'Clients']:
                try:
                    id_name = self._leech_driver.get_credible_id_name(id_type)
                except MissingObjectException:
                    id_name = 'unknown'
                changed_target.append({
                    'id_source': id_source,
                    'id_type': record['record_type'],
                    'id_name': id_name,
                    'id_value': Decimal(record['record_id'])
                })
        source_data = {
            'change_date_utc': extracted_data['change_date_utc'],
            'change_description': extracted_data['change_description'],
            'change_date': extracted_data['change_date'],
            'action': extracted_data['action'],
            'action_id': change_category.get_action_id(extracted_data['action']),
            'id_source': id_source,
            'id_type': 'ChangeLog',
            'id_name': 'change_date_utc',
            'done_by': extracted_data['done_by']
        }
        returned_data = {
            'source': source_data,
            'changed_target': changed_target
        }
        return returned_data

    def _set_fruit(self, remote_objects):
        for change_object in remote_objects:
            id_value = change_object['source']['change_date_utc']
            pairs = {
                'id_source': change_object['source']['id_source'],
                'id_type': change_object['source']['id_type'],
                'id_name': change_object['source']['id_name']
            }
            identifier_stem = IdentifierStem('vertex', 'ChangeLog', pairs)
            is_working_already = self._mark_objects_as_working(id_value, identifier_stem, change_object)
            if not is_working_already:
                order_args = (identifier_stem, id_value, change_object, self._schema_entry)
                transform_order = TransformObjectOrder(*order_args)
                self._transform_queue.add_order(transform_order)
        self._transform_queue.push_orders()

    def _mark_objects_as_working(self, id_value, identifier_stem, extracted_data):
        return self._leech_driver.put_vertex_driven_seed(
            extracted_data, identifier_stem=identifier_stem, id_value=id_value)

    @classmethod
    def _perform_change_detail_extraction(cls, driver, driving_id_value, local_max_value,
                                          change_category, action_id, driving_identifier_stem):
        change_details = driver.get_change_details(**{
            'driving_id_type': driving_identifier_stem.get('id_type'),
            'driving_id_name': driving_identifier_stem.get('id_name'),
            'driving_id_value': driving_id_value,
            'local_max_value': local_max_value,
            'category_id': change_category.category_id,
            'action_id': int(action_id)
        })
        return change_details

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
        extraction_properties = self._schema_entry.extract[self._id_source]
        mapping = extraction_properties['mapping']
        id_source_mapping = mapping.get(self._id_source, mapping['default'])
        object_mapping = id_source_mapping[self._driving_identifier_stem.object_type]
        return object_mapping
