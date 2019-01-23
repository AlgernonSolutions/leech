import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from dateutil.tz import tzlocal

from tests.units.test_data import patches
from toll_booth.alg_obj.aws.gentlemen.events.events import Event
from toll_booth.alg_obj.serializers import AlgDecoder, AlgEncoder


class MockSwfEvent:
    @classmethod
    def for_workflow_started(cls, input_string, task_list_name, flow_type, **kwargs):
        attributes = {
            'workflowExecutionStartedEventAttributes': {
                'input': input_string,
                'taskList': {'name': task_list_name},
                'workflowType': {
                    'name': flow_type,
                    'version': kwargs.get('version', '1')
                }
            }
        }
        return Event(1, 'WorkflowExecutionStarted', datetime.utcnow(), attributes)


_lambda_labor_params = [
    ('get_remote_ids',
     '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1547632256.888168.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1547632257.494224.json"}}}}}'),
    ('get_local_ids',
     '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1547632256.888168.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1547632257.494224.json"}}}}}'),
    ('put_new_ids',
     '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1547632610.605488.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1547632611.220036.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1547614647.89166.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1547614625.421932.json"}}}}}'),
]

mule_team_params = (
    'get_enrichment_for_change_action',
    '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548208189.812885.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548208190.72171.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548190253.498273.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548190253.499673.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548190253.499693.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4851!1548190253.934165.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548190306.370741.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548190281.620162.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action325!1548190343.01072.json"}}}}}'
)

work_change_type_params = (
    'work_remote_id_change_type',
    '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548276521.454877.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548276522.666301.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548258531.124235.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548258531.125017.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548258531.12503.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id5177!1548258531.6202.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type10!1548258539.876893.json"}}}}}'
)

bad_subtask_events = [{
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 12, 892000, tzinfo=tzlocal()),
    'eventType': 'WorkflowExecutionStarted',
    'eventId': 1,
    'workflowExecutionStartedEventAttributes': {
        'input': '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type2!1548267432.225058.json"}}}}}',
        'executionStartToCloseTimeout': '86400',
        'taskStartToCloseTimeout': '300',
        'childPolicy': 'TERMINATE',
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'workflowType': {
            'name': 'work_remote_id_change_type',
            'version': '3'
        },
        'parentWorkflowExecution': {
            'workflowId': 'work_id-4991-f-31',
            'runId': '22DL1b/OpFHHkaRGOTKdS6ix9j/tJ0W8INUivOZXCc4hI='
        },
        'parentInitiatedEventId': 19,
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 12, 892000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskScheduled',
    'eventId': 2,
    'decisionTaskScheduledEventAttributes': {
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'startToCloseTimeout': '300'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 15, 996000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskStarted',
    'eventId': 3,
    'decisionTaskStartedEventAttributes': {
        'scheduledEventId': 2
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 17, 704000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskCompleted',
    'eventId': 4,
    'decisionTaskCompletedEventAttributes': {
        'scheduledEventId': 2,
        'startedEventId': 3
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 17, 704000, tzinfo=tzlocal()),
    'eventType': 'LambdaFunctionScheduled',
    'eventId': 5,
    'lambdaFunctionScheduledEventAttributes': {
        'id': 'get_local_max-work_type-Print-work_id-4991-f-31',
        'name': 'leech-lambda-labor',
        'input': '{"task_name": "get_local_max_change_type_value", "task_args": {"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type2!1548267432.225058.json"}}}}}, "register_results": false}',
        'decisionTaskCompletedEventId': 4
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 17, 737000, tzinfo=tzlocal()),
    'eventType': 'LambdaFunctionStarted',
    'eventId': 6,
    'lambdaFunctionStartedEventAttributes': {
        'scheduledEventId': 5
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 20, 274000, tzinfo=tzlocal()),
    'eventType': 'LambdaFunctionCompleted',
    'eventId': 7,
    'lambdaFunctionCompletedEventAttributes': {
        'scheduledEventId': 5,
        'startedEventId': 6,
        'result': '"{\\"_alg_class\\": \\"StoredData\\", \\"_alg_module\\": \\"toll_booth.alg_obj.aws.snakes.snakes\\", \\"value\\": {\\"pointer\\": \\"the-leech#cache/get_local_max_change_type_value!1548267439.957246.json\\"}}"'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 20, 274000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskScheduled',
    'eventId': 8,
    'decisionTaskScheduledEventAttributes': {
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'startToCloseTimeout': '300'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 20, 303000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskStarted',
    'eventId': 9,
    'decisionTaskStartedEventAttributes': {
        'scheduledEventId': 8
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 23, 463000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskCompleted',
    'eventId': 10,
    'decisionTaskCompletedEventAttributes': {
        'scheduledEventId': 8,
        'startedEventId': 9
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 23, 463000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 11,
    'markerRecordedEventAttributes': {
        'markerName': 'checkpoint',
        'details': '{"get_local_max-work_type-Print-work_id-4991-f-31": "{\\"_alg_class\\": \\"StoredData\\", \\"_alg_module\\": \\"toll_booth.alg_obj.aws.snakes.snakes\\", \\"value\\": {\\"pointer\\": \\"the-leech#cache/get_local_max_change_type_value!1548267439.957246.json\\"}}"}',
        'decisionTaskCompletedEventId': 10
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 23, 463000, tzinfo=tzlocal()),
    'eventType': 'ActivityTaskScheduled',
    'eventId': 12,
    'activityTaskScheduledEventAttributes': {
        'activityType': {
            'name': 'work_remote_id_change_type',
            'version': '2'
        },
        'activityId': 'work_remote_id_change_type-work_type-Print-work_id-4991-f-31',
        'input': '{"task_name": "work_remote_id_change_type", "task_args": {"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type2!1548267432.225058.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548267439.957246.json"}}}}}, "register_results": true}',
        'scheduleToStartTimeout': 'NONE',
        'scheduleToCloseTimeout': 'NONE',
        'startToCloseTimeout': 'NONE',
        'taskList': {
            'name': 'credible'
        },
        'decisionTaskCompletedEventId': 10,
        'heartbeatTimeout': 'NONE'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 23, 492000, tzinfo=tzlocal()),
    'eventType': 'ActivityTaskStarted',
    'eventId': 13,
    'activityTaskStartedEventAttributes': {
        'scheduledEventId': 12
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 33, 289000, tzinfo=tzlocal()),
    'eventType': 'ActivityTaskCompleted',
    'eventId': 14,
    'activityTaskCompletedEventAttributes': {
        'result': '{"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548267452.799156.json"}}',
        'scheduledEventId': 12,
        'startedEventId': 13
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 33, 289000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskScheduled',
    'eventId': 15,
    'decisionTaskScheduledEventAttributes': {
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'startToCloseTimeout': '300'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 33, 333000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskStarted',
    'eventId': 16,
    'decisionTaskStartedEventAttributes': {
        'scheduledEventId': 15
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 40, 372000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskCompleted',
    'eventId': 17,
    'decisionTaskCompletedEventAttributes': {
        'scheduledEventId': 15,
        'startedEventId': 16
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 40, 372000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 18,
    'markerRecordedEventAttributes': {
        'markerName': 'checkpoint',
        'details': '{"work_remote_id_change_type-work_type-Print-work_id-4991-f-31": "{\\"_alg_class\\": \\"StoredData\\", \\"_alg_module\\": \\"toll_booth.alg_obj.aws.snakes.snakes\\", \\"value\\": {\\"pointer\\": \\"the-leech#cache/work_remote_id_change_type!1548267452.799156.json\\"}}"}',
        'decisionTaskCompletedEventId': 17
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 40, 372000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 19,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ACCESS CLIENT PRINTABLE RECORD-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ACCESSCLIENTPRINTABLERECORD-work_type-Print-work_id-4991-f-31!!7043a", "is_close": false}',
        'decisionTaskCompletedEventId': 17
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 40, 372000, tzinfo=tzlocal()),
    'eventType': 'StartChildWorkflowExecutionInitiated',
    'eventId': 20,
    'startChildWorkflowExecutionInitiatedEventAttributes': {
        'workflowId': 'work_action-ACCESS CLIENT PRINTABLE RECORD-work_type-Print-work_id-4991-f-31',
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'input': '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548267452.799156.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548267439.957246.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action80!1548267459.478851.json"}}}}}',
        'executionStartToCloseTimeout': '86400',
        'taskList': {
            'name': 'work_action-ACCESS CLIENT PRINTABLE RECORD-work_type-Print-work_id-4991-f-31'
        },
        'decisionTaskCompletedEventId': 17,
        'childPolicy': 'TERMINATE',
        'taskStartToCloseTimeout': 'NONE',
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 40, 372000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 21,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-CLIENT EDUCATION PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-CLIENTEDUCATIONPRINTED-work_type-Print-work_id-4991-f-31!!e9daf73888", "is_close": false}',
        'decisionTaskCompletedEventId': 17
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 40, 372000, tzinfo=tzlocal()),
    'eventType': 'StartChildWorkflowExecutionInitiated',
    'eventId': 22,
    'startChildWorkflowExecutionInitiatedEventAttributes': {
        'workflowId': 'work_action-CLIENT EDUCATION PRINTED-work_type-Print-work_id-4991-f-31',
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'input': '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548267452.799156.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548267439.957246.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action670!1548267459.478935.json"}}}}}',
        'executionStartToCloseTimeout': '86400',
        'taskList': {
            'name': 'work_action-CLIENT EDUCATION PRINTED-work_type-Print-work_id-4991-f-31'
        },
        'decisionTaskCompletedEventId': 17,
        'childPolicy': 'TERMINATE',
        'taskStartToCloseTimeout': 'NONE',
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 40, 421000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionStarted',
    'eventId': 23,
    'childWorkflowExecutionStartedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-ACCESS CLIENT PRINTABLE RECORD-work_type-Print-work_id-4991-f-31',
            'runId': '22Dmu1lBHGzZwJSTurZvXiijzAE0UHKIkLxd66JRgM26Y='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 20
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 40, 421000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskScheduled',
    'eventId': 24,
    'decisionTaskScheduledEventAttributes': {
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'startToCloseTimeout': '300'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 40, 486000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskStarted',
    'eventId': 25,
    'decisionTaskStartedEventAttributes': {
        'scheduledEventId': 24
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 42, 64000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionStarted',
    'eventId': 26,
    'childWorkflowExecutionStartedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-CLIENT EDUCATION PRINTED-work_type-Print-work_id-4991-f-31',
            'runId': '22uXfHwNmgT2qiQNEahhs8Ps/IY9q4BgSlgrYnjLtH2+8='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 22
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 42, 64000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskScheduled',
    'eventId': 27,
    'decisionTaskScheduledEventAttributes': {
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'startToCloseTimeout': '300'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 47, 200000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionCompleted',
    'eventId': 28,
    'childWorkflowExecutionCompletedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-ACCESS CLIENT PRINTABLE RECORD-work_type-Print-work_id-4991-f-31',
            'runId': '22Dmu1lBHGzZwJSTurZvXiijzAE0UHKIkLxd66JRgM26Y='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 20,
        'startedEventId': 23
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 49, 401000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskCompleted',
    'eventId': 29,
    'decisionTaskCompletedEventAttributes': {
        'scheduledEventId': 24,
        'startedEventId': 25
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 49, 401000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 30,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ELABS RESULTS PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ELABSRESULTSPRINTED-work_type-Print-work_id-4991-f-31!!a3f921aba5d04", "is_close": false}',
        'decisionTaskCompletedEventId': 29
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 49, 401000, tzinfo=tzlocal()),
    'eventType': 'StartChildWorkflowExecutionInitiated',
    'eventId': 31,
    'startChildWorkflowExecutionInitiatedEventAttributes': {
        'workflowId': 'work_action-ELABS RESULTS PRINTED-work_type-Print-work_id-4991-f-31',
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'input': '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548267452.799156.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548267439.957246.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action712!1548267468.89859.json"}}}}}',
        'executionStartToCloseTimeout': '86400',
        'taskList': {
            'name': 'work_action-ELABS RESULTS PRINTED-work_type-Print-work_id-4991-f-31'
        },
        'decisionTaskCompletedEventId': 29,
        'childPolicy': 'TERMINATE',
        'taskStartToCloseTimeout': 'NONE',
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 49, 426000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionCompleted',
    'eventId': 32,
    'childWorkflowExecutionCompletedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-CLIENT EDUCATION PRINTED-work_type-Print-work_id-4991-f-31',
            'runId': '22uXfHwNmgT2qiQNEahhs8Ps/IY9q4BgSlgrYnjLtH2+8='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 22,
        'startedEventId': 26
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 49, 461000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionStarted',
    'eventId': 33,
    'childWorkflowExecutionStartedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-ELABS RESULTS PRINTED-work_type-Print-work_id-4991-f-31',
            'runId': '22dDnrPoBzZ6DVBVDoQkQE1za+w/pZ6Zh5tUVeZoJYnj0='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 31
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 49, 537000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskStarted',
    'eventId': 34,
    'decisionTaskStartedEventAttributes': {
        'scheduledEventId': 27
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 55, 600000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionCompleted',
    'eventId': 35,
    'childWorkflowExecutionCompletedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-ELABS RESULTS PRINTED-work_type-Print-work_id-4991-f-31',
            'runId': '22dDnrPoBzZ6DVBVDoQkQE1za+w/pZ6Zh5tUVeZoJYnj0='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 31,
        'startedEventId': 33
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 55, 600000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskScheduled',
    'eventId': 36,
    'decisionTaskScheduledEventAttributes': {
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'startToCloseTimeout': '300'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 59, 785000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskCompleted',
    'eventId': 37,
    'decisionTaskCompletedEventAttributes': {
        'scheduledEventId': 27,
        'startedEventId': 34
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 59, 785000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 38,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-EPCS PRESCRIPTION PRINT-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-EPCSRXPRINT-work_type-Print-work_id-4991-f-31!!078ef4a15b32481f8bb92", "is_close": false}',
        'decisionTaskCompletedEventId': 37
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 59, 785000, tzinfo=tzlocal()),
    'eventType': 'StartChildWorkflowExecutionInitiated',
    'eventId': 39,
    'startChildWorkflowExecutionInitiatedEventAttributes': {
        'workflowId': 'work_action-EPCS PRESCRIPTION PRINT-work_type-Print-work_id-4991-f-31',
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'input': '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548267452.799156.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548267439.957246.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action542!1548267478.958669.json"}}}}}',
        'executionStartToCloseTimeout': '86400',
        'taskList': {
            'name': 'work_action-EPCS PRESCRIPTION PRINT-work_type-Print-work_id-4991-f-31'
        },
        'decisionTaskCompletedEventId': 37,
        'childPolicy': 'TERMINATE',
        'taskStartToCloseTimeout': 'NONE',
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 59, 785000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 40,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ACCESS CLIENT PRINTABLE RECORD-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ACCESSCLIENTPRINTABLERECORD-work_type-Print-work_id-4991-f-31!!7043a", "is_close": true}',
        'decisionTaskCompletedEventId': 37
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 59, 785000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 41,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-CLIENT EDUCATION PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-CLIENTEDUCATIONPRINTED-work_type-Print-work_id-4991-f-31!!e9daf73888", "is_close": true}',
        'decisionTaskCompletedEventId': 37
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 59, 854000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionStarted',
    'eventId': 42,
    'childWorkflowExecutionStartedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-EPCS PRESCRIPTION PRINT-work_type-Print-work_id-4991-f-31',
            'runId': '22rQB7RNT15j6ZdjxQ5RzshlEn0FywBQEfOfb8aFT2kDM='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 39
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 17, 59, 906000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskStarted',
    'eventId': 43,
    'decisionTaskStartedEventAttributes': {
        'scheduledEventId': 36
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 6, 899000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionCompleted',
    'eventId': 44,
    'childWorkflowExecutionCompletedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-EPCS PRESCRIPTION PRINT-work_type-Print-work_id-4991-f-31',
            'runId': '22rQB7RNT15j6ZdjxQ5RzshlEn0FywBQEfOfb8aFT2kDM='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 39,
        'startedEventId': 42
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 6, 899000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskScheduled',
    'eventId': 45,
    'decisionTaskScheduledEventAttributes': {
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'startToCloseTimeout': '300'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 10, 664000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskCompleted',
    'eventId': 46,
    'decisionTaskCompletedEventAttributes': {
        'scheduledEventId': 36,
        'startedEventId': 43
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 10, 664000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 47,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-PRINT CLIENT MEDICATIONS-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-PRINTCLIENTMEDICATIONS-work_type-Print-work_id-4991-f-31!!2f51dfb5bb", "is_close": false}',
        'decisionTaskCompletedEventId': 46
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 10, 664000, tzinfo=tzlocal()),
    'eventType': 'StartChildWorkflowExecutionInitiated',
    'eventId': 48,
    'startChildWorkflowExecutionInitiatedEventAttributes': {
        'workflowId': 'work_action-PRINT CLIENT MEDICATIONS-work_type-Print-work_id-4991-f-31',
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'input': '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548267452.799156.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548267439.957246.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action325!1548267489.806082.json"}}}}}',
        'executionStartToCloseTimeout': '86400',
        'taskList': {
            'name': 'work_action-PRINT CLIENT MEDICATIONS-work_type-Print-work_id-4991-f-31'
        },
        'decisionTaskCompletedEventId': 46,
        'childPolicy': 'TERMINATE',
        'taskStartToCloseTimeout': 'NONE',
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 10, 664000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 49,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ACCESS CLIENT PRINTABLE RECORD-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ACCESSCLIENTPRINTABLERECORD-work_type-Print-work_id-4991-f-31!!7043a", "is_close": true}',
        'decisionTaskCompletedEventId': 46
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 10, 664000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 50,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-CLIENT EDUCATION PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-CLIENTEDUCATIONPRINTED-work_type-Print-work_id-4991-f-31!!e9daf73888", "is_close": true}',
        'decisionTaskCompletedEventId': 46
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 10, 664000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 51,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ELABS RESULTS PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ELABSRESULTSPRINTED-work_type-Print-work_id-4991-f-31!!a3f921aba5d04", "is_close": true}',
        'decisionTaskCompletedEventId': 46
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 10, 728000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionStarted',
    'eventId': 52,
    'childWorkflowExecutionStartedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-PRINT CLIENT MEDICATIONS-work_type-Print-work_id-4991-f-31',
            'runId': '22gJpelun1Y9vSVJu+4uFDpyFLwANON1OS47/vaz8XnT0='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 48
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 10, 778000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskStarted',
    'eventId': 53,
    'decisionTaskStartedEventAttributes': {
        'scheduledEventId': 45
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 17, 366000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionCompleted',
    'eventId': 54,
    'childWorkflowExecutionCompletedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-PRINT CLIENT MEDICATIONS-work_type-Print-work_id-4991-f-31',
            'runId': '22gJpelun1Y9vSVJu+4uFDpyFLwANON1OS47/vaz8XnT0='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 48,
        'startedEventId': 52
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 17, 366000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskScheduled',
    'eventId': 55,
    'decisionTaskScheduledEventAttributes': {
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'startToCloseTimeout': '300'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 23, 494000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskCompleted',
    'eventId': 56,
    'decisionTaskCompletedEventAttributes': {
        'scheduledEventId': 45,
        'startedEventId': 53
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 23, 494000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 57,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-PRINT CLIENT PLANNER-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-PRINTCLIENTPLANNER-work_type-Print-work_id-4991-f-31!!fe625f552ff441", "is_close": false}',
        'decisionTaskCompletedEventId': 56
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 23, 494000, tzinfo=tzlocal()),
    'eventType': 'StartChildWorkflowExecutionInitiated',
    'eventId': 58,
    'startChildWorkflowExecutionInitiatedEventAttributes': {
        'workflowId': 'work_action-PRINT CLIENT PLANNER-work_type-Print-work_id-4991-f-31',
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'input': '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548267452.799156.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548267439.957246.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action324!1548267502.690919.json"}}}}}',
        'executionStartToCloseTimeout': '86400',
        'taskList': {
            'name': 'work_action-PRINT CLIENT PLANNER-work_type-Print-work_id-4991-f-31'
        },
        'decisionTaskCompletedEventId': 56,
        'childPolicy': 'TERMINATE',
        'taskStartToCloseTimeout': 'NONE',
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 23, 494000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 59,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ACCESS CLIENT PRINTABLE RECORD-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ACCESSCLIENTPRINTABLERECORD-work_type-Print-work_id-4991-f-31!!7043a", "is_close": true}',
        'decisionTaskCompletedEventId': 56
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 23, 494000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 60,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-CLIENT EDUCATION PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-CLIENTEDUCATIONPRINTED-work_type-Print-work_id-4991-f-31!!e9daf73888", "is_close": true}',
        'decisionTaskCompletedEventId': 56
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 23, 494000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 61,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ELABS RESULTS PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ELABSRESULTSPRINTED-work_type-Print-work_id-4991-f-31!!a3f921aba5d04", "is_close": true}',
        'decisionTaskCompletedEventId': 56
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 23, 494000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 62,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-EPCS PRESCRIPTION PRINT-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-EPCSRXPRINT-work_type-Print-work_id-4991-f-31!!078ef4a15b32481f8bb92", "is_close": true}',
        'decisionTaskCompletedEventId': 56
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 23, 574000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionStarted',
    'eventId': 63,
    'childWorkflowExecutionStartedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-PRINT CLIENT PLANNER-work_type-Print-work_id-4991-f-31',
            'runId': '22SZ23mb9EPsS3Kd70UeITSosU3lCVsvg39LZgH4wG6TI='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 58
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 23, 622000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskStarted',
    'eventId': 64,
    'decisionTaskStartedEventAttributes': {
        'scheduledEventId': 55
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 30, 860000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionCompleted',
    'eventId': 65,
    'childWorkflowExecutionCompletedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-PRINT CLIENT PLANNER-work_type-Print-work_id-4991-f-31',
            'runId': '22SZ23mb9EPsS3Kd70UeITSosU3lCVsvg39LZgH4wG6TI='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 58,
        'startedEventId': 63
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 30, 860000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskScheduled',
    'eventId': 66,
    'decisionTaskScheduledEventAttributes': {
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'startToCloseTimeout': '300'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 39, 803000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskCompleted',
    'eventId': 67,
    'decisionTaskCompletedEventAttributes': {
        'scheduledEventId': 55,
        'startedEventId': 64
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 39, 803000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 68,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-PRINT CLIENT PORTAL VISIT-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-PRINTCLIENTPORTALVISIT-work_type-Print-work_id-4991-f-31!!cff21452bb", "is_close": false}',
        'decisionTaskCompletedEventId': 67
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 39, 803000, tzinfo=tzlocal()),
    'eventType': 'StartChildWorkflowExecutionInitiated',
    'eventId': 69,
    'startChildWorkflowExecutionInitiatedEventAttributes': {
        'workflowId': 'work_action-PRINT CLIENT PORTAL VISIT-work_type-Print-work_id-4991-f-31',
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'input': '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548267452.799156.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548267439.957246.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action540!1548267518.867054.json"}}}}}',
        'executionStartToCloseTimeout': '86400',
        'taskList': {
            'name': 'work_action-PRINT CLIENT PORTAL VISIT-work_type-Print-work_id-4991-f-31'
        },
        'decisionTaskCompletedEventId': 67,
        'childPolicy': 'TERMINATE',
        'taskStartToCloseTimeout': 'NONE',
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 39, 803000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 70,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ACCESS CLIENT PRINTABLE RECORD-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ACCESSCLIENTPRINTABLERECORD-work_type-Print-work_id-4991-f-31!!7043a", "is_close": true}',
        'decisionTaskCompletedEventId': 67
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 39, 803000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 71,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-CLIENT EDUCATION PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-CLIENTEDUCATIONPRINTED-work_type-Print-work_id-4991-f-31!!e9daf73888", "is_close": true}',
        'decisionTaskCompletedEventId': 67
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 39, 803000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 72,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ELABS RESULTS PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ELABSRESULTSPRINTED-work_type-Print-work_id-4991-f-31!!a3f921aba5d04", "is_close": true}',
        'decisionTaskCompletedEventId': 67
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 39, 803000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 73,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-EPCS PRESCRIPTION PRINT-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-EPCSRXPRINT-work_type-Print-work_id-4991-f-31!!078ef4a15b32481f8bb92", "is_close": true}',
        'decisionTaskCompletedEventId': 67
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 39, 803000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 74,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-PRINT CLIENT MEDICATIONS-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-PRINTCLIENTMEDICATIONS-work_type-Print-work_id-4991-f-31!!2f51dfb5bb", "is_close": true}',
        'decisionTaskCompletedEventId': 67
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 39, 862000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionStarted',
    'eventId': 75,
    'childWorkflowExecutionStartedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-PRINT CLIENT PORTAL VISIT-work_type-Print-work_id-4991-f-31',
            'runId': '22+ARxrS+JiwBXceSOj1Kty9lB98MUc1EGa2k2E+81LLw='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 69
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 39, 924000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskStarted',
    'eventId': 76,
    'decisionTaskStartedEventAttributes': {
        'scheduledEventId': 66
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 46, 296000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionCompleted',
    'eventId': 77,
    'childWorkflowExecutionCompletedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-PRINT CLIENT PORTAL VISIT-work_type-Print-work_id-4991-f-31',
            'runId': '22+ARxrS+JiwBXceSOj1Kty9lB98MUc1EGa2k2E+81LLw='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 69,
        'startedEventId': 75
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 46, 296000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskScheduled',
    'eventId': 78,
    'decisionTaskScheduledEventAttributes': {
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'startToCloseTimeout': '300'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 56, 457000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskCompleted',
    'eventId': 79,
    'decisionTaskCompletedEventAttributes': {
        'scheduledEventId': 66,
        'startedEventId': 76
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 56, 457000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 80,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-PRINT CLIENT PROFILE-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-PRINTCLIENTPROFILE-work_type-Print-work_id-4991-f-31!!6d55b4542e2041", "is_close": false}',
        'decisionTaskCompletedEventId': 79
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 56, 457000, tzinfo=tzlocal()),
    'eventType': 'StartChildWorkflowExecutionInitiated',
    'eventId': 81,
    'startChildWorkflowExecutionInitiatedEventAttributes': {
        'workflowId': 'work_action-PRINT CLIENT PROFILE-work_type-Print-work_id-4991-f-31',
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'input': '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548267452.799156.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548267439.957246.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action321!1548267535.50032.json"}}}}}',
        'executionStartToCloseTimeout': '86400',
        'taskList': {
            'name': 'work_action-PRINT CLIENT PROFILE-work_type-Print-work_id-4991-f-31'
        },
        'decisionTaskCompletedEventId': 79,
        'childPolicy': 'TERMINATE',
        'taskStartToCloseTimeout': 'NONE',
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 56, 457000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 82,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ACCESS CLIENT PRINTABLE RECORD-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ACCESSCLIENTPRINTABLERECORD-work_type-Print-work_id-4991-f-31!!7043a", "is_close": true}',
        'decisionTaskCompletedEventId': 79
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 56, 457000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 83,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-CLIENT EDUCATION PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-CLIENTEDUCATIONPRINTED-work_type-Print-work_id-4991-f-31!!e9daf73888", "is_close": true}',
        'decisionTaskCompletedEventId': 79
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 56, 457000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 84,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ELABS RESULTS PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ELABSRESULTSPRINTED-work_type-Print-work_id-4991-f-31!!a3f921aba5d04", "is_close": true}',
        'decisionTaskCompletedEventId': 79
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 56, 457000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 85,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-EPCS PRESCRIPTION PRINT-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-EPCSRXPRINT-work_type-Print-work_id-4991-f-31!!078ef4a15b32481f8bb92", "is_close": true}',
        'decisionTaskCompletedEventId': 79
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 56, 457000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 86,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-PRINT CLIENT MEDICATIONS-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-PRINTCLIENTMEDICATIONS-work_type-Print-work_id-4991-f-31!!2f51dfb5bb", "is_close": true}',
        'decisionTaskCompletedEventId': 79
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 56, 457000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 87,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-PRINT CLIENT PLANNER-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-PRINTCLIENTPLANNER-work_type-Print-work_id-4991-f-31!!fe625f552ff441", "is_close": true}',
        'decisionTaskCompletedEventId': 79
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 56, 516000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionStarted',
    'eventId': 88,
    'childWorkflowExecutionStartedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-PRINT CLIENT PROFILE-work_type-Print-work_id-4991-f-31',
            'runId': '22hVCac6Id3VjPuUZyUb/SZoZD+L1G0m3b5vYomQbNMTY='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 81
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 18, 56, 581000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskStarted',
    'eventId': 89,
    'decisionTaskStartedEventAttributes': {
        'scheduledEventId': 78
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 19, 4, 525000, tzinfo=tzlocal()),
    'eventType': 'ChildWorkflowExecutionCompleted',
    'eventId': 90,
    'childWorkflowExecutionCompletedEventAttributes': {
        'workflowExecution': {
            'workflowId': 'work_action-PRINT CLIENT PROFILE-work_type-Print-work_id-4991-f-31',
            'runId': '22hVCac6Id3VjPuUZyUb/SZoZD+L1G0m3b5vYomQbNMTY='
        },
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'initiatedEventId': 81,
        'startedEventId': 88
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 19, 4, 525000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskScheduled',
    'eventId': 91,
    'decisionTaskScheduledEventAttributes': {
        'taskList': {
            'name': 'work_type-Print-work_id-4991-f-31'
        },
        'startToCloseTimeout': '300'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 19, 13, 955000, tzinfo=tzlocal()),
    'eventType': 'DecisionTaskCompleted',
    'eventId': 92,
    'decisionTaskCompletedEventAttributes': {
        'scheduledEventId': 78,
        'startedEventId': 89
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 19, 13, 955000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 93,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-PRINT CLIENT VISIT-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-PRINTCLIENTVISIT-work_type-Print-work_id-4991-f-31!!d39d9e66880a4e75", "is_close": false}',
        'decisionTaskCompletedEventId': 92
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 19, 13, 955000, tzinfo=tzlocal()),
    'eventType': 'StartChildWorkflowExecutionInitiated',
    'eventId': 94,
    'startChildWorkflowExecutionInitiatedEventAttributes': {
        'workflowId': 'work_action-PRINT CLIENT VISIT-work_type-Print-work_id-4991-f-31',
        'workflowType': {
            'name': 'work_remote_id_change_action',
            'version': '3'
        },
        'input': '{"_alg_class": "TaskArguments", "_alg_module": "toll_booth.alg_obj.aws.gentlemen.tasks", "value": {"_arguments": {"fungus": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/fungus!1548285327.770378.json"}}, "command_fungi": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/command_fungi!1548285329.73385.json"}}, "get_local_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_ids!1548190224.918203.json"}}, "get_remote_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_remote_ids!1548190210.075505.json"}}, "put_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/put_new_ids!1548267333.349892.json"}}, "link_new_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/link_new_ids!1548267333.35111.json"}}, "unlink_old_ids": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/unlink_old_ids!1548267333.351129.json"}}, "pull_change_types": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/pull_change_types!1548190240.516952.json"}}, "build_mapping": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/build_mapping!1548190247.936402.json"}}, "work_remote_id": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id4991!1548267333.816659.json"}}, "work_remote_id_change_type": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_type!1548267452.799156.json"}}, "get_local_max_change_type_value": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/get_local_max_change_type_value!1548267439.957246.json"}}, "work_remote_id_change_action": {"_alg_class": "StoredData", "_alg_module": "toll_booth.alg_obj.aws.snakes.snakes", "value": {"pointer": "the-leech#cache/work_remote_id_change_action36!1548267552.965352.json"}}}}}',
        'executionStartToCloseTimeout': '86400',
        'taskList': {
            'name': 'work_action-PRINT CLIENT VISIT-work_type-Print-work_id-4991-f-31'
        },
        'decisionTaskCompletedEventId': 92,
        'childPolicy': 'TERMINATE',
        'taskStartToCloseTimeout': 'NONE',
        'lambdaRole': 'arn:aws:iam::803040539655:role/swf-lambda'
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 19, 13, 955000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 95,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ACCESS CLIENT PRINTABLE RECORD-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ACCESSCLIENTPRINTABLERECORD-work_type-Print-work_id-4991-f-31!!7043a", "is_close": true}',
        'decisionTaskCompletedEventId': 92
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 19, 13, 955000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 96,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-CLIENT EDUCATION PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-CLIENTEDUCATIONPRINTED-work_type-Print-work_id-4991-f-31!!e9daf73888", "is_close": true}',
        'decisionTaskCompletedEventId': 92
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 19, 13, 955000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 97,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-ELABS RESULTS PRINTED-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-ELABSRESULTSPRINTED-work_type-Print-work_id-4991-f-31!!a3f921aba5d04", "is_close": true}',
        'decisionTaskCompletedEventId': 92
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 19, 13, 955000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 98,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-EPCS PRESCRIPTION PRINT-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-EPCSRXPRINT-work_type-Print-work_id-4991-f-31!!078ef4a15b32481f8bb92", "is_close": true}',
        'decisionTaskCompletedEventId': 92
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 19, 13, 955000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 99,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-PRINT CLIENT MEDICATIONS-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-PRINTCLIENTMEDICATIONS-work_type-Print-work_id-4991-f-31!!2f51dfb5bb", "is_close": true}',
        'decisionTaskCompletedEventId': 92
    }
}, {
    'eventTimestamp': datetime(2019, 1, 23, 18, 19, 13, 955000, tzinfo=tzlocal()),
    'eventType': 'MarkerRecorded',
    'eventId': 100,
    'markerRecordedEventAttributes': {
        'markerName': 'ruffian',
        'details': '{"task_identifier": "work_action-PRINT CLIENT PLANNER-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-PRINTCLIENTPLANNER-work_type-Print-work_id-4991-f-31!!fe625f552ff441", "is_close": true}',
        'decisionTaskCompletedEventId': 92
    }
},
    {
        'eventTimestamp': datetime(2019, 1, 23, 18, 19, 13, 955000, tzinfo=tzlocal()),
        'eventType': 'MarkerRecorded',
        'eventId': 101,
        'markerRecordedEventAttributes': {
            'markerName': 'ruffian',
            'details': '{"task_identifier": "work_action-PRINT CLIENT PORTAL VISIT-work_type-Print-work_id-4991-f-31", "execution_arn": "arn:aws:states:us-east-1:803040539655:execution:decider:work_action-PRINTCLIENTPORTALVISIT-work_type-Print-work_id-4991-f-31!!cff21452bb", "is_close": true}',
            'decisionTaskCompletedEventId': 92
        }
    }, {
        'eventTimestamp': datetime(2019, 1, 23, 18, 19, 14, 23000, tzinfo=tzlocal()),
        'eventType': 'ChildWorkflowExecutionStarted',
        'eventId': 102,
        'childWorkflowExecutionStartedEventAttributes': {
            'workflowExecution': {
                'workflowId': 'work_action-PRINT CLIENT VISIT-work_type-Print-work_id-4991-f-31',
                'runId': '22+zLdpLxCWsBEsgYpIZk4/64lFiXUhvcZgaRzHi3mNbc='
            },
            'workflowType': {
                'name': 'work_remote_id_change_action',
                'version': '3'
            },
            'initiatedEventId': 94
        }
    }, {
        'eventTimestamp': datetime(2019, 1, 23, 18, 19, 14, 75000, tzinfo=tzlocal()),
        'eventType': 'DecisionTaskStarted',
        'eventId': 103,
        'decisionTaskStartedEventAttributes': {
            'scheduledEventId': 91
        }
    }, {
        'eventTimestamp': datetime(2019, 1, 23, 18, 19, 20, 5000, tzinfo=tzlocal()),
        'eventType': 'ChildWorkflowExecutionCompleted',
        'eventId': 104,
        'childWorkflowExecutionCompletedEventAttributes': {
            'workflowExecution': {
                'workflowId': 'work_action-PRINT CLIENT VISIT-work_type-Print-work_id-4991-f-31',
                'runId': '22+zLdpLxCWsBEsgYpIZk4/64lFiXUhvcZgaRzHi3mNbc='
            },
            'workflowType': {
                'name': 'work_remote_id_change_action', 'version': '3'
            },
            'initiatedEventId': 94,
            'startedEventId': 102
        }
    }, {
        'eventTimestamp': datetime(2019, 1, 23, 18, 19, 20, 5000, tzinfo=tzlocal()),
        'eventType': 'DecisionTaskScheduled',
        'eventId': 105,
        'decisionTaskScheduledEventAttributes': {
            'taskList': {
                'name': 'work_type-Print-work_id-4991-f-31'
            },
            'startToCloseTimeout': '300'}},
    {'eventTimestamp': datetime(2019, 1, 23, 18, 24, 14, 76000, tzinfo=tzlocal()),
     'eventType': 'DecisionTaskTimedOut', 'eventId': 106,
     'decisionTaskTimedOutEventAttributes': {'timeoutType': 'START_TO_CLOSE', 'scheduledEventId': 91,
                                             'startedEventId': 103}},
    {'eventTimestamp': datetime(2019, 1, 23, 18, 24, 14, 108000, tzinfo=tzlocal()),
     'eventType': 'DecisionTaskStarted', 'eventId': 107,
     'decisionTaskStartedEventAttributes': {'scheduledEventId': 105}}
]


@pytest.fixture
def initial_decision_history():
    return MagicMock()


@pytest.fixture
def mock_versions():
    version_patch = patches.get_version_patch()
    mock_version = version_patch.start()
    yield mock_version
    version_patch.stop()


@pytest.fixture
def mock_config():
    config_patch = patches.get_config_patch()
    mock_config = config_patch.start()
    mock_config.concurrency.return_value = 1
    yield mock_config
    config_patch.stop()


@pytest.fixture(params=_lambda_labor_params)
def lambda_labor_arg(request):
    params = request.param
    task_args = json.loads(params[1], cls=AlgDecoder)
    return {'task_name': params[0], 'task_args': task_args}


@pytest.fixture
def mule_team_arg():
    task_args = json.loads(mule_team_params[1], cls=AlgDecoder)
    return {'task_name': 'get_enrichment_for_change_action', 'task_args': task_args}


@pytest.fixture
def work_change_type_arg():
    task_args = json.loads(work_change_type_params[1], cls=AlgDecoder)
    return {'task_name': 'work_remote_id_change_type', 'task_args': task_args}


@pytest.fixture
def mock_activity_poll():
    poll_patch = patches.get_poll_patch()
    mock_poll = poll_patch.start()
    yield mock_poll
    poll_patch.stop()


@pytest.fixture
def mock_decision_poll():
    poll_patch = patches.get_poll_patch(True)
    mock_poll = poll_patch.start()
    yield mock_poll
    poll_patch.stop()


@pytest.fixture
def spiked_decision_poll():
    poll_patch = patches.get_poll_patch(True)
    mock_poll = poll_patch.start()
    from toll_booth.alg_obj.aws.gentlemen.events.history import WorkflowHistory
    poll_response = {
        'events': bad_subtask_events,
        'workflowType': {'name': 'test'},
        'taskToken': '123NotaTaskToken',
        'workflowExecution': {
            'workflowId': '123321',
            'runId': '98766789'
        }
    }
    work_history = WorkflowHistory.parse_from_poll('TheLeech', poll_response)
    yield mock_poll
    poll_patch.stop()
