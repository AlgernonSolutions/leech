import pytest

from toll_booth.alg_tasks.remote_tasks.explode import explode
from toll_booth.alg_tasks.toll_booth import exploded

insert_test = {
    "Records": [
        {
            "eventID": "212f75f1d852f1c3906ed112f558cd89",
            "eventName": "INSERT",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "us-east-1",
            "dynamodb": {
                "ApproximateCreationDateTime": 1540835220.0,
                "Keys": {
                    "sid_value": {
                        "S": "#vertex#ExternalId#{\"id_source\": \"MBI\"}#{\"id_type\": \"Employees\"}#{\"id_name\": \"emp_id\"}#"
                    },
                    "identifier_stem": {
                      "S": "#SOURCE#vertex#ExternalId#{\"id_source\": \"MBI\"}#{\"id_type\": \"Employees\"}#{\"id_name\": \"emp_id\"}#"
                    }
                  },
                "NewImage": {
                  "sid_value": {
                    "S":  "#SOURCE#vertex#ExternalId#{\"id_source\": \"MBI\"}#{\"id_type\": \"Employees\"}#{\"id_name\": \"emp_id\"}#"
                  },
                  "identifier_stem": {
                    "S": "#SOURCE#vertex#ExternalId#{\"id_source\": \"MBI\"}#{\"id_type\": \"Employees\"}#{\"id_name\": \"emp_id\"}#"
                  },
                  "extractor_function_names": {
                    "M": {
                      "extraction": {
                        "S": "leech-extract-crediblews"
                      },
                      "index_query": {
                        "S": "leech-extract-crediblews"
                      }
                    }
                  }
                },
                "SequenceNumber": "17851100000000038001690247",
                "SizeBytes": 508,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            },
            "eventSourceARN": "arn:aws:dynamodb:us-east-1:803040539655:table/GraphObjects/stream/2018-10-26T16:35:36.212"
        }
    ]
}
remove_test = {
    'Records': [
        {
            'eventID': 'dd66fa1956871ff82afae74a46025f73',
            'eventName': 'REMOVE',
            'eventVersion': '1.1',
            'eventSource': 'aws:dynamodb',
            'awsRegion': 'us-east-1',
            'dynamodb': {
                'ApproximateCreationDateTime': 1540835220.0,
                'Keys': {
                    'sid_value': {
                        'S': '#vertex#ExternalId#{id_source:MBI}#{id_type:Employees}#{id_name:emp_id}#'
                    },
                    'identifier_stem': {
                        'S': '#SOURCE#vertex#ExternalId#{id_source:MBI}#{id_type:Employees}#{id_name:emp_id}#'
                    }
                },
                'OldImage': {
                    'sid_value': {
                        'S': '#vertex#ExternalId#{id_source:MBI}#{id_type:Employees}#{id_name:emp_id}#'
                    },
                    'identifier_stem': {
                        'S': '#SOURCE#vertex#ExternalId#{id_source:MBI}#{id_type:Employees}#{id_name:emp_id}#'
                    },
                    'extractor_function_names': {
                        'M': {
                            'extraction': {
                                'S': 'leech-extract-crediblews'
                            },
                            'index_query': {
                                'S': 'leech-extract-crediblews'
                            }
                        }
                    }
                },
                'SequenceNumber': '17851200000000038001690286',
                'SizeBytes': 448,
                'StreamViewType': 'NEW_AND_OLD_IMAGES'
            },
            'eventSourceARN': 'arn:aws:dynamodb:us-east-1:803040539655:table/GraphObjects/stream/2018-10-26T16:35:36.212'
        }
    ]
}


class TestExploded:
    @pytest.mark.exploded
    def test_exploded(self):
        results = exploded(insert_test, [])
        print(results)

    @pytest.mark.explode
    @pytest.mark.parametrize('event', [
        insert_test, remove_test
    ])
    def test_empty_explode(self, event):
        explode_event = {
            'task_name': 'explode',
            'task_args': {
                'records': event['Records']
            }
        }

        explode(explode_event['task_args'], context={})
