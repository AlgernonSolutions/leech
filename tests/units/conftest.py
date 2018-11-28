import os
from collections import OrderedDict
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from mock import MagicMock

from tests.units.test_data import patches
from tests.units.test_data.actor_data import *
from tests.units.test_data.assimilation_orders import first_identifiable_assimilation_order
from tests.units.test_data.assimilation_orders.assimilation_events import change_to_stub_changelog_assimilation_order
from tests.units.test_data.assimilation_results import generate_assimilation_results_set
from tests.units.test_data.data_setup.boto import intercept
from tests.units.test_data.dynamo_stream_events import *
from tests.units.test_data.patches import get_leech_driver_patch, get_function_patch
from tests.units.test_data.schema_generator import get_schema_entry
from tests.units.test_data.transform_results import generate_transform_results
from tests.units.test_data.vertex_generator import generate_potential_vertex
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem


@pytest.fixture(scope='session')
def blank_table():
    table_name = os.getenv('blank_table_name', 'TestObjects')
    import boto3
    client = boto3.client('dynamodb')

    try:
        _create_table(client, table_name)
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceInUseException':
            raise e
        try:
            client.delete_table(TableName=table_name)
        except ClientError:
            pass
        waiter = client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name, WaiterConfig={'Delay': 10})
        _create_table(client, table_name)

    yield

    client.delete_table(TableName=table_name)


def _create_table(client, table_name):
    indexed_field = os.getenv('indexed_field', 'id_value')
    partition_key = os.getenv('partition_key')
    sort_key = os.getenv('sort_key')
    index_name = os.getenv('index_name')

    client.create_table(
        AttributeDefinitions=[
            {'AttributeName': partition_key, 'AttributeType': 'S'},
            {'AttributeName': sort_key, 'AttributeType': 'S'},
            {'AttributeName': indexed_field, 'AttributeType': 'N'},
        ],
        TableName=table_name,
        KeySchema=[
            {'AttributeName': partition_key, 'KeyType': 'HASH'},
            {'AttributeName': sort_key, 'KeyType': 'RANGE'}
        ],
        GlobalSecondaryIndexes=[{
            'IndexName': index_name,
            'KeySchema': [
                {'AttributeName': partition_key, 'KeyType': 'HASH'},
                {'AttributeName': indexed_field, 'KeyType': 'RANGE'}
            ],
            'Projection': {'ProjectionType': 'KEYS_ONLY'},
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 3,
                'WriteCapacityUnits': 3
            }
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 3,
            'WriteCapacityUnits': 3
        }
    )
    waiter = client.get_waiter('table_exists')
    waiter.wait(TableName=table_name, WaiterConfig={'Delay': 10})


@pytest.fixture
def put_vertexes():
    import boto3
    vertex_keys = []
    table_names = []

    def _put_vertexes(table_name, vertex, id_range, sort_key, vertex_key):
        import boto3

        client = boto3.resource('dynamodb').Table(table_name)
        table_names.append(table_name)
        for id_value in id_range:
            new_key = vertex_key.copy()
            new_vertex = vertex.copy()
            new_vertex['id_value'] = id_value
            new_vertex[sort_key] = str(id_value)
            new_key[sort_key] = str(id_value)
            client.put_item(Item=new_vertex)
            vertex_keys.append(new_key)

    yield _put_vertexes

    for name in table_names:
        test_client = boto3.resource('dynamodb').Table(name)
        for key in vertex_keys:
            test_client.delete_item(Key=key)


@pytest.fixture
def put_vertex():
    import boto3
    vertex_keys = []
    table_names = []

    def _put_vertex(table_name, vertex, vertex_key):
        import boto3

        table_names.append(table_name)
        vertex_keys.append(vertex_key)
        client = boto3.resource('dynamodb').Table(table_name)
        client.put_item(Item=vertex)

    yield _put_vertex

    for name in table_names:
        test_client = boto3.resource('dynamodb').Table(name)
        for key in vertex_keys:
            test_client.delete_item(Key=key)


@pytest.fixture
def delete_test_edge():
    import boto3

    def _delete_edge(table_name, edge_key):
        client = boto3.resource('dynamodb').Table(table_name)
        client.delete_item(Key=edge_key)

    yield _delete_edge


@pytest.fixture
def delete_vertex():
    def _delete_vertex(table_name, vertex_key):
        import boto3
        client = boto3.resource('dynamodb').Table(table_name)
        client.delete_item(Key=vertex_key)

    yield _delete_vertex


@pytest.fixture(params=[
    ext_id_identifier_stem,
    change_identifier_stem
])
def identifier_stem(request):
    return request.param


@pytest.fixture(params=[
    ('ExternalId', ext_id_identifier_stem, ext_id_id_value, credible_ws_extractor, credible_ws_extractor_function),
    ('Change', change_identifier_stem, change_id_value, credible_ws_extractor, credible_ws_extractor_function)
])
def extraction_order(request):
    from toll_booth.alg_obj.graph.schemata.schema_entry import SchemaEntry
    from toll_booth.alg_obj.forge.comms.orders import ExtractObjectOrder
    from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem

    params = request.param
    identifier_stem = IdentifierStem.from_raw(params[1])
    schema_entry = SchemaEntry.get(params[0])
    extraction_properties = identifier_stem.for_extractor
    schema_extraction_properties = schema_entry.extract[params[3]]
    extraction_properties.update(schema_extraction_properties.extraction_properties)
    id_value = params[2]
    extractor_function_name = params[4]
    return ExtractObjectOrder(identifier_stem, id_value, extractor_function_name, extraction_properties, schema_entry)


@pytest.fixture(params=[
    ('ExternalId', ext_id_identifier_stem, ext_id_id_value, ext_id_extracted_data),
    ('Change', change_identifier_stem, change_id_value, change_extracted_data),
    ('Change', change_identifier_stem, second_change_id_value, second_change_extracted_data),
])
def transform_order(request):
    from toll_booth.alg_obj.forge.comms.orders import TransformObjectOrder

    params = request.param
    schema_entry = get_schema_entry(params[0])
    return TransformObjectOrder(params[1], params[2], params[3], schema_entry)


@pytest.fixture(params=[
    first_identifiable_assimilation_order,
])
def identifiable_assimilate_order(request):
    return request.param


@pytest.fixture(params=[
    change_to_stub_changelog_assimilation_order,
    # first_stub_assimilate_order,
    # second_stub_assimilate_order
])
def unidentifiable_assimilate_order(request):
    return request.param


@pytest.fixture(params=[
    ('MBI', 'Employees', 'emp_id', 'vertex', 'ExternalId'),
    ('MBI', 'Clients', 'client_id', 'vertex', 'ExternalId'),
    ('MBI', 'ClientVisit', 'clientvisit_id', 'vertex', 'ExternalId'),
    ('MBI', 'ChangeLogDetail', 'changelogdetail_id', 'vertex', 'Change')
])
def credible_ws_payload(request):
    params = request.param
    return {
        'step_name': 'index_query',
        'step_args': {
            'id_source': params[0],
            'id_type': params[1],
            'id_name': params[2],
            'graph_type': params[3],
            'object_type': params[4]
        }
    }


@pytest.fixture(params=[
    insert_event, remove_event, graphing_event, assimilated_event
])
def dynamo_stream_event(request):
    return request.param


@pytest.fixture
def assimilated_vertex_stream_event():
    return assimilated_event


@pytest.fixture(params=[
    ('1002', '#vertex#ExternalId#{"id_source": "MBI", "id_type": "Clients", "id_name": "client_id"}#')
])
def load_order(request):
    params = request.param
    return {
        'task_name': 'load',
        'task_args': {
            'keys': {
                'sid_value': {'S': params[0]},
                'identifier_stem': {
                    'S': params[1]
                }
            },
            'table_name': 'GraphObjects'
        }
    }


@pytest.fixture(params=[
    'ExternalId', 'Change', 'ChangeLogEntry'
])
def potential_vertex(request, has_missing=False):
    test_vertex = generate_potential_vertex(request.param, has_missing)
    return test_vertex


@pytest.fixture(params=[
    'ExternalId', 'Change', 'ChangeLogEntry'
])
def stub_potential_vertex(request):
    stub_vertex = generate_potential_vertex(request.param, True)
    return stub_vertex


@pytest.fixture
def mock_context():
    context = MagicMock(name='context')
    context.function_name = 'test_function'
    context.invoked_function_arn = 'test_function_arn'
    context.aws_request_id = '12344_request_id'
    context.get_remaining_time_in_millis.side_effect = [1000001, 500001, 250000, 0]
    return context


@pytest.fixture
def silence_x_ray():
    patch(patches.x_ray_patch_begin).start()
    patch(patches.x_ray_patch_end).start()
    yield
    patch(patches.x_ray_patch_begin).start()
    patch(patches.x_ray_patch_end).start()


@pytest.fixture
def mock_neptune():
    fish_sticks = patch(patches.neptune_patch).start()
    yield fish_sticks
    patch(patches.neptune_patch).start()


@pytest.fixture
def borg_test_environment():
    driver_patches = []

    def _build_environment(findable_vertex_type=False):
        driver_functions = ['find_potential_vertexes', 'set_assimilation_results', 'set_assimilated_vertex']
        mocks = {}
        edge_regulator_patch = get_function_patch('edge_regulator', 'generate_potential_edge')
        edge_regulator_mock = edge_regulator_patch.start()
        mocks['generate_potential_edge'] = edge_regulator_mock
        driver_patches.append(edge_regulator_patch)
        for function_name in driver_functions:
            function_patch = get_leech_driver_patch(function_name)
            mock_object = function_patch.start()
            driver_patches.append(function_patch)
            mocks[function_name] = mock_object
        if findable_vertex_type:
            found_vertexes = [
                MagicMock('first_found_vertex'),
                MagicMock('second_found_vertex')
            ]
            mocks['find_potential_vertexes'].return_value = found_vertexes
        else:
            mocks['find_potential_vertexes'].return_value = []
        return mocks

    yield _build_environment

    for driver_patch in driver_patches:
        driver_patch.stop()


@pytest.fixture
def robot_test_environment():
    boto_patch = patches.get_boto_patch()
    schema_patch = patches.get_function_patch('schema_entry', 'get')
    mock_schema = schema_patch.start()
    mock_schema.side_effect = intercept
    mock_boto = boto_patch.start()
    mock_boto.side_effect = intercept
    yield mock_boto
    boto_patch.stop()
    schema_patch.stop()


@pytest.fixture
def dynamo_test_environment():
    boto_patch = patches.get_boto_patch()
    mock_boto = boto_patch.start()
    mock_boto.side_effect = intercept
    yield mock_boto
    boto_patch.stop()


@pytest.fixture
def test_working_ids(identifier_stem):
    return IdentifierStem.from_raw(identifier_stem), range(1001, 1010)


@pytest.fixture
def test_id(identifier_stem):
    return IdentifierStem.from_raw(identifier_stem), 1001


@pytest.fixture(params=[
    True, False
])
def test_transform_results(potential_vertex, request):
    has_potentials = request.param
    return generate_transform_results(potential_vertex, has_potentials)


@pytest.fixture(params=[
    'ExternalId', 'Change', 'ChangeLogEntry'
])
def test_assimilation_generator(request):
    test_vertex = generate_potential_vertex(request.param)
    return generate_assimilation_results_set(test_vertex)


@pytest.fixture
def test_assimilation_results(test_assimilation_generator):
    return test_assimilation_generator


@pytest.fixture(params=[
    {'task_name': 'load', 'task_args': {'keys': {'sid_value': '220', 'identifier_stem': '#vertex#ChangeLogEntry::stub#{"id_source": "MBI", "id_type": null, "id_name": null}#'}}},
    {'task_name': 'load', 'task_args': {'keys': {'sid_value': '1227', 'identifier_stem': '#vertex#Change#{"id_source": "MBI", "id_type": "ChangeLogDetail", "id_name": "changelogdetail_id"}#'}}},
    {'task_name': 'load', 'task_args': {'keys': {'sid_value': '1234', 'identifier_stem': '#vertex#Change#{"id_source": "MBI", "id_type": "ChangeLogDetail", "id_name": "changelogdetail_id"}#'}}},
    {'task_name': 'load', 'task_args': {'keys': {'sid_value': '1224', 'identifier_stem': '#vertex#Change#{"id_source": "MBI", "id_type": "ChangeLogDetail", "id_name": "changelogdetail_id"}#'}}},
])
def load_task(request):
    return request.param


@pytest.fixture(params=[
    ('Client', 'Clients', 'client_id'),
    ('Employee', 'Employees', 'emp_id'),
    ('Encounter', 'ClientVisit', 'clientvisit_id')
])
def vd_identifier_stem(request):
    params = request.param
    identifier_stem = IdentifierStem('vertex', params[0], {'id_source': 'Algernon', 'id_type': params[1], 'id_name': params[2]})
    return identifier_stem


@pytest.fixture(params=[
    ('ChangeLog', 'MBI', 'Clients', 'client_id', 5198, 4, datetime.datetime(2018, 8, 12)),
    ('ChangeLog', 'MBI', 'Clients', 'client_id', 1001, 4, datetime.datetime(2018, 8, 12)),
])
def monitored_object_identifier_stem(request):
    params = request.param
    paired_identifiers = OrderedDict()
    paired_identifiers['id_source'] = params[1]
    paired_identifiers['id_type'] = params[2]
    paired_identifiers['id_name'] = params[3]
    paired_identifiers['id_value'] = params[4]
    paired_identifiers['data_dict_id'] = params[5]
    identifier_stem = IdentifierStem('vertex', params[0], paired_identifiers)
    return identifier_stem, params[6]


@pytest.fixture
def employee_ext_id_identifier_stem():
    return IdentifierStem('vertex', 'ExternalId', {'id_source': 'MBI', 'id_type': 'Employees', 'id_name': 'emp_id'})


@pytest.fixture(params=[
    ('ChangeLog', 'MBI', 'Employees', 'emp_id'),
    ('ChangeLog', 'MBI', 'Clients', 'client_id')
])
def propagated_identifier_stem(request):
    params = request.param
    source_stem = IdentifierStem('vertex', params[0], {'id_source': params[1]})
    driving_stem = IdentifierStem('vertex', 'ExternalId', {'id_source': params[1], 'id_type': params[2], 'id_name': params[3]})
    return {
        'identifier_stem': source_stem,
        'driving_identifier_stem': driving_stem
    }


@pytest.fixture(params=[
    ('Cubeta', 'J', 'MBI')
])
def employee_name(request):
    return request.param


@pytest.fixture
def mock_schema():
    schema_patch = patches.get_function_patch('schema_entry', 'get')
    mock_schema = schema_patch.start()
    mock_schema.side_effect = intercept
    yield
    schema_patch.stop()


@pytest.fixture(params=[
    ()
])
def mock_vertex_driven_event(request):
    params = request.param


@pytest.fixture(params=[
    ('MBI', 'ChangeLog', 'Employees', 'emp_id', 5400)
])
def specified_identifier_stem(request):
    params = request.param
    identifiers = {
        'id_source': params[0],
        'id_type': params[2],
        'id_name': params[3]
    }
    identifier_stem = IdentifierStem('vertex', params[1], {'id_source': params[0]})
    driving_stem = IdentifierStem('vertex', 'ExternalId', identifiers)
    specified_stem = identifier_stem.specify(driving_stem, params[4])
    return specified_stem


@pytest.fixture
def propagation_id():
    return 'f07365392bb7495b94f56c04f0bec1f2'


@pytest.fixture(params=[
    {
        'identifier': {
            'identifier_stem': IdentifierStem('vertex', 'ChangeLog', {
                'identifier_stem': IdentifierStem('vertex', 'ExternalId', {
                    'id_source': 'MBI', 'id_type': 'Employees', 'id_name': 'emp_id'
                })
            }),
            'id_value': 5400,
            'local_max_values': {
                1: None
            }
        },
        'id_source': 'MBI'
    }
])
def credible_fe_args(request):
    params = request.param
    params['mapping'] = {
      "DCDBH": {
        "ExternalId": {
          "Employees": {
            "internal_name": "Employee ID",
            "alg_name": "emp_id"
          },
          "Clients": {
            "internal_name": "Client ID",
            "alg_name": "client_id"
          }
        },
        "Clients": {
          "Date": {
            "name": "change_date",
            "mutation": None
          },
          "UTCDate": {
            "name": "change_date_utc",
            "mutation": None
          },
          "Description": {
            "name": "change_description",
            "mutation": None
          },
          "Action": {
            "name": "action",
            "mutation": None
          },
          "Service ID": {
            "name": "clientvisit_id",
            "mutation": None
          },
          "Record": {
            "name": "record",
            "mutation": "split_record_id"
          },
          "Consumer Name": {
            "name": "client_id",
            "mutation": "get_client_id"
          },
          "User": {
            "name": "emp_id",
            "mutation": None
          }
        },
        "Employees": {
          "Date": {
            "name": "change_date",
            "mutation": None
          },
          "UTCDate": {
            "name": "change_date_utc",
            "mutation": None
          },
          "Description": {
            "name": "change_description",
            "mutation": None
          },
          "Action": {
            "name": "action",
            "mutation": None
          },
          "Service ID": {
            "name": "clientvisit_id",
            "mutation": None
          },
          "Record": {
            "name": "record",
            "mutation": "split_record_id"
          },
          "Consumer Name": {
            "name": "client_id",
            "mutation": "get_client_id"
          },
          "User": {
            "name": "emp_id",
            "mutation": None
          }
        }
      },
      "MBI": {
        "ExternalId": {
          "Employees": {
            "internal_name": "Employee ID",
            "alg_name": "emp_id"
          },
          "Clients": {
            "internal_name": "Client ID",
            "alg_name": "client_id"
          }
        },
        "Clients": {
          "Date": {
            "name": "change_date",
            "mutation": None
          },
          "UTCDate": {
            "name": "change_date_utc",
            "mutation": None
          },
          "Description": {
            "name": "change_description",
            "mutation": None
          },
          "Action": {
            "name": "action",
            "mutation": None
          },
          "Service ID": {
            "name": "clientvisit_id",
            "mutation": None
          },
          "Record": {
            "name": "record",
            "mutation": "split_record_id"
          },
          "Consumer Name": {
            "name": "client_id",
            "mutation": "get_client_id"
          },
          "User": {
            "name": "emp_id",
            "mutation": None
          }
        },
        "Employees": {
          "Date": {
            "name": "change_date",
            "mutation": None
          },
          "UTCDate": {
            "name": "change_date_utc",
            "mutation": None
          },
          "Description": {
            "name": "change_description",
            "mutation": None
          },
          "Action": {
            "name": "action",
            "mutation": None
          },
          "Service ID": {
            "name": "clientvisit_id",
            "mutation": None
          },
          "Record": {
            "name": "record",
            "mutation": "split_record_id"
          },
          "Consumer Name": {
            "name": "client_id",
            "mutation": "get_client_id"
          },
          "User": {
            "name": "emp_id",
            "mutation": None
          }
        }
      },
      "default": {
        "ExternalId": {
          "Employees": {
            "internal_name": "Employee ID",
            "alg_name": "emp_id"
          },
          "Clients": {
            "internal_name": "Client ID",
            "alg_name": "client_id"
          }
        },
        "Clients": {
          "Date": {
            "name": "change_date",
            "mutation": None
          },
          "UTCDate": {
            "name": "change_date_utc",
            "mutation": None
          },
          "Description": {
            "name": "change_description",
            "mutation": None
          },
          "Action": {
            "name": "action",
            "mutation": None
          },
          "Service ID": {
            "name": "clientvisit_id",
            "mutation": None
          },
          "Record": {
            "name": "record",
            "mutation": "split_record_id"
          },
          "Client Name": {
            "name": "client_id",
            "mutation": "get_client_id"
          },
          "User": {
            "name": "emp_id",
            "mutation": None
          }
        },
        "Employees": {
          "Date": {
            "name": "change_date",
            "mutation": None
          },
          "UTCDate": {
            "name": "change_date_utc",
            "mutation": None
          },
          "Description": {
            "name": "change_description",
            "mutation": None
          },
          "Action": {
            "name": "action",
            "mutation": None
          },
          "Service ID": {
            "name": "clientvisit_id",
            "mutation": None
          },
          "Record": {
            "name": "record",
            "mutation": "split_record_id"
          },
          "Consumer Name": {
            "name": "client_id",
            "mutation": "get_client_id"
          },
          "User": {
            "name": "emp_id",
            "mutation": None
          }
        }
      }
    }
    return params
