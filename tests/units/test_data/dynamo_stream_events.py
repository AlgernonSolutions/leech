insert_event = {
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

remove_event = {
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

graphing_event = {
    'Records': [
        {
            'eventID': '8bb16f0f8c0a16779058099f7e079b18',
            'eventName': 'MODIFY',
            'eventVersion': '1.1',
            'eventSource': 'aws:dynamodb',
            'awsRegion': 'us-east-1',
            'dynamodb': {
                'ApproximateCreationDateTime': 1540914060.0,
                'Keys': {
                    'sid_value': {'S': '1051'},
                    'identifier_stem': {
                        'S': '#vertex#ExternalId#{"id_source": "MBI", "id_type": "Clients", "id_name": "client_id"}#'
                    }
                },
                'NewImage': {
                    'internal_id': {'S': '6ca3b20280d386e1a75a8424c2f13cbb'},
                    'monitoring_clear_time': {'N': '1540914058.8288629055023193359375'},
                    'object_type': {'S': 'ExternalId'},
                    'transformation_clear_time': {'N': '1540914074.3816521167755126953125'},
                    'is_edge': {'BOOL': False},
                    'extraction_clear_time': {'N': '1540914070.5077641010284423828125'},
                    'identifier_stem': {
                        'S': '#vertex#ExternalId#{"id_source": "MBI", "id_type": "Clients", "id_name": "client_id"}#'
                    },
                    'completed': {'BOOL': False},
                    'id_value_field': {'S': 'id_value'},
                    'sid_value': {'S': '1051'},
                    'disposition': {'S': 'graphing'},
                    'last_stage_seen': {'S': 'transformation'},
                    'if_missing': {'NULL': True},
                    'object_properties': {
                        'M': {
                            'id_source': {'S': 'MBI'},
                            'id_type': {'S': 'Clients'},
                            'id_value': {'N': '1051'},
                            'id_name': {'S': 'client_id'}
                        }
                    },
                    'last_seen_time': {'N': '1540914074.3816521167755126953125'},
                    'id_value': {'N': '1051'}
                },
                'OldImage': {
                    'sid_value': {'S': '1051'},
                    'disposition': {'S': 'working'},
                    'last_stage_seen': {'S': 'extraction'},
                    'monitoring_clear_time': {'N': '1540914058.8288629055023193359375'},
                    'object_type': {'S': 'ExternalId'},
                    'is_edge': {'BOOL': False},
                    'extraction_clear_time': {'N': '1540914070.5077641010284423828125'},
                    'identifier_stem': {
                        'S': '#vertex#ExternalId#{"id_source": "MBI", "id_type": "Clients", "id_name": "client_id"}#'
                    },
                    'completed': {'BOOL': False},
                    'last_seen_time': {'N': '1540914070.5077641010284423828125'},
                    'id_value': {'N': '1051'}
                },
                'SequenceNumber': '23662000000000025813842570',
                'SizeBytes': 942,
                'StreamViewType': 'NEW_AND_OLD_IMAGES'
            },
            'eventSourceARN': 'arn:aws:dynamodb:us-east-1:803040539655:table/GraphObjects/stream/2018-10-26T16:35:36.212'
        }
    ]
}
