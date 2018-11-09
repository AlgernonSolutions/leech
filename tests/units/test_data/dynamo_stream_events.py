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

assimilated_event = {
    'eventID': '7e81ea3c3e62467614dbb9d37036b827',
    'eventName': 'MODIFY',
    'eventVersion': '1.1',
    'eventSource': 'aws:dynamodb',
    'awsRegion': 'us-east-1',
    'dynamodb': {
        'ApproximateCreationDateTime': 1541781720.0,
        'Keys': {
            'sid_value': {
                'S': '1246'
            },
            'identifier_stem': {
                'S': '#vertex#Change#{"id_source": "MBI", "id_type": "ChangeLogDetail", "id_name": "changelogdetail_id"}#'
            }
        },
        'NewImage': {
            'potentials': {
                'M': {
                    '_change_': {
                        'M': {
                            'rule_entry': {
                                'M': {
                                    'if_absent': {
                                        'S': 'stub'
                                    },
                                    'target_constants': {
                                        'L': []
                                    },
                                    'target_specifiers': {
                                        'L': [{
                                            'M': {
                                                'specifier_type': {
                                                    'S': 'shared_property'
                                                },
                                                'shared_properties': {
                                                    'NULL': True
                                                },
                                                'extracted_properties': {
                                                    'L': [{
                                                        'S': 'id_source'
                                                    }, {
                                                        'S': 'changelog_id'
                                                    }
                                                    ]
                                                },
                                                'function_name': {
                                                    'NULL': True
                                                },
                                                'specifier_name': {
                                                    'S': 'changelog_source'
                                                }
                                            }
                                        }
                                        ]
                                    },
                                    'inbound': {
                                        'BOOL': True
                                    },
                                    'target_type': {
                                        'S': 'ChangeLogEntry'
                                    },
                                    'edge_type': {
                                        'S': '_change_'
                                    }
                                }
                            },
                            'assimilated': {
                                'BOOL': True
                            },
                            'potential_vertex': {
                                'M': {
                                    'sid_value': {
                                        'S': '649'
                                    },
                                    'internal_id': {
                                        'S': '6b6071c7958baca0fb44215560452e9c'
                                    },
                                    'object_type': {
                                        'S': 'ChangeLogEntry'
                                    },
                                    'identifier_stem': {
                                        'S': "['id_source', 'id_type', 'id_name']"
                                    },
                                    'object_properties': {
                                        'M': {
                                            'change_date_utc': {
                                                'NULL': True
                                            },
                                            'change_description': {
                                                'NULL': True
                                            },
                                            'change_date': {
                                                'NULL': True
                                            },
                                            'id_source': {
                                                'S': 'MBI'
                                            },
                                            'id_type': {
                                                'NULL': True
                                            },
                                            'changelog_id': {
                                                'N': '649'
                                            },
                                            'id_name': {
                                                'NULL': True
                                            }
                                        }
                                    },
                                    'id_value': {
                                        'N': '649'
                                    }
                                }
                            }
                        }
                    },
                    '_changed_': {
                        'M': {
                            'rule_entry': {
                                'M': {
                                    'if_absent': {
                                        'S': 'create'
                                    },
                                    'target_constants': {
                                        'L': [{
                                            'M': {
                                                'constant_name': {
                                                    'S': 'id_source'
                                                },
                                                'constant_value': {
                                                    'S': 'source.id_source'
                                                }
                                            }
                                        }
                                        ]
                                    },
                                    'target_specifiers': {
                                        'L': [{
                                            'M': {
                                                'specifier_type': {
                                                    'S': 'function'
                                                },
                                                'shared_properties': {
                                                    'NULL': True
                                                },
                                                'extracted_properties': {
                                                    'L': [{
                                                        'S': 'id_source'
                                                    }, {
                                                        'S': 'id_type'
                                                    }, {
                                                        'S': 'id_name'
                                                    }, {
                                                        'S': 'id_value'
                                                    }
                                                    ]
                                                },
                                                'function_name': {
                                                    'S': 'derive_change_targeted'
                                                },
                                                'specifier_name': {
                                                    'S': 'changed_target'
                                                }
                                            }
                                        }
                                        ]
                                    },
                                    'inbound': {
                                        'BOOL': False
                                    },
                                    'target_type': {
                                        'S': 'ExternalId'
                                    },
                                    'edge_type': {
                                        'S': '_changed_'
                                    }
                                }
                            },
                            'identified_vertexes': {
                                'L': [{
                                    'M': {
                                        'edge': {
                                            'M': {
                                                'sid_value': {
                                                    'S': 'b81f222b801662a9446c37f6c44c4c5d'
                                                },
                                                'internal_id': {
                                                    'S': 'b81f222b801662a9446c37f6c44c4c5d'
                                                },
                                                'from_object': {
                                                    'S': 'a9e4709ba338250b0a55485ba373979f'
                                                },
                                                'to_object': {
                                                    'S': '14671e379b32b7c875e28b0440012389'
                                                },
                                                'identifier_stem': {
                                                    'S': '#edge#_changed_#{}#'
                                                },
                                                'object_properties': {
                                                    'M': {
                                                        'change_date_utc': {
                                                            'S': '2014-07-30T02:05:42+0000'
                                                        }
                                                    }
                                                },
                                                'edge_label': {
                                                    'S': '_changed_'
                                                }
                                            }
                                        },
                                        'vertex': {
                                            'M': {
                                                'sid_value': {
                                                    'S': '28'
                                                },
                                                'internal_id': {
                                                    'S': '14671e379b32b7c875e28b0440012389'
                                                },
                                                'object_type': {
                                                    'S': 'ExternalId'
                                                },
                                                'identifier_stem': {
                                                    'S': '#vertex#ExternalId#{"id_source": "MBI", "id_type": "ExportBuilder", "id_name": "exportbuilder_id"}#'
                                                },
                                                'object_properties': {
                                                    'M': {
                                                        'id_source': {
                                                            'S': 'MBI'
                                                        },
                                                        'id_type': {
                                                            'S': 'ExportBuilder'
                                                        },
                                                        'id_value': {
                                                            'N': '28'
                                                        },
                                                        'id_name': {
                                                            'S': 'exportbuilder_id'
                                                        }
                                                    }
                                                },
                                                'id_value': {
                                                    'N': '28'
                                                }
                                            }
                                        }
                                    }
                                }
                                ]
                            },
                            'assimilated': {
                                'BOOL': True
                            },
                            'potential_vertex': {
                                'M': {
                                    'sid_value': {
                                        'S': '28'
                                    },
                                    'internal_id': {
                                        'S': '14671e379b32b7c875e28b0440012389'
                                    },
                                    'object_type': {
                                        'S': 'ExternalId'
                                    },
                                    'identifier_stem': {
                                        'S': '#vertex#ExternalId#{"id_source": "MBI", "id_type": "ExportBuilder", "id_name": "exportbuilder_id"}#'
                                    },
                                    'object_properties': {
                                        'M': {
                                            'id_source': {
                                                'S': 'MBI'
                                            },
                                            'id_type': {
                                                'S': 'ExportBuilder'
                                            },
                                            'id_value': {
                                                'N': '28'
                                            },
                                            'id_name': {
                                                'S': 'exportbuilder_id'
                                            }
                                        }
                                    },
                                    'id_value': {
                                        'N': '28'
                                    }
                                }
                            }
                        }
                    }
                }
            },
            'internal_id': {
                'S': 'a9e4709ba338250b0a55485ba373979f'
            },
            'object_type': {
                'S': 'Change'
            },
            'identifier_stem': {
                'S': '#vertex#Change#{"id_source": "MBI", "id_type": "ChangeLogDetail", "id_name": "changelogdetail_id"}#'
            },
            'completed': {
                'BOOL': False
            },
            'last_time_seen': {
                'N': '1541781744.3145580291748046875'
            },
            'extracted_data': {
                'S': '{"source": {"id_name": "changelogdetail_id", "id_type": "ChangeLogDetail", "id_source": "MBI", "detail_id": 1246, "changelog_id": 649, "data_dict_id": "", "detail_one_value": "", "detail_one": "", "detail_two": ""}, "changed_target": [{"change_date_utc": {"_alg_class": "datetime", "value": 1406685942.947}, "client_id": "", "clientvisit_id": "", "emp_id": 3796, "record_id": 28, "record_type": "ExportBuilder", "primarykey_name": "exportbuilder_id"}]}'
            },
            'sid_value': {
                'S': '1246'
            },
            'disposition': {
                'S': 'working'
            },
            'last_stage_seen': {
                'S': 'assimilation'
            },
            'progress': {
                'M': {
                    'extraction': {
                        'N': '1541781737.7540950775146484375'
                    },
                    'monitoring': {
                        'N': '1541781669.0441429615020751953125'
                    },
                    'transformation': {
                        'N': '1541781738.447886943817138671875'
                    },
                    'assimilation': {
                        'M': {
                            '_changed_': {
                                'N': '1541781744.3145580291748046875'
                            }
                        }
                    }
                }
            },
            'object_properties': {
                'M': {
                    'id_source': {
                        'S': 'MBI'
                    },
                    'detail_one': {
                        'NULL': True
                    },
                    'data_dict_id': {
                        'NULL': True
                    },
                    'id_type': {
                        'S': 'ChangeLogDetail'
                    },
                    'changelog_id': {
                        'N': '649'
                    },
                    'detail_id': {
                        'N': '1246'
                    },
                    'id_name': {
                        'S': 'changelogdetail_id'
                    },
                    'detail_two': {
                        'NULL': True
                    },
                    'detail_one_value': {
                        'NULL': True
                    }
                }
            },
            'id_value': {
                'N': '1246'
            }
        },
        'OldImage': {
            'potentials': {
                'M': {
                    '_change_': {
                        'M': {
                            'rule_entry': {
                                'M': {
                                    'if_absent': {
                                        'S': 'stub'
                                    },
                                    'target_constants': {
                                        'L': []
                                    },
                                    'target_specifiers': {
                                        'L': [{
                                            'M': {
                                                'specifier_type': {
                                                    'S': 'shared_property'
                                                },
                                                'shared_properties': {
                                                    'NULL': True
                                                },
                                                'extracted_properties': {
                                                    'L': [{
                                                        'S': 'id_source'
                                                    }, {
                                                        'S': 'changelog_id'
                                                    }
                                                    ]
                                                },
                                                'function_name': {
                                                    'NULL': True
                                                },
                                                'specifier_name': {
                                                    'S': 'changelog_source'
                                                }
                                            }
                                        }
                                        ]
                                    },
                                    'inbound': {
                                        'BOOL': True
                                    },
                                    'target_type': {
                                        'S': 'ChangeLogEntry'
                                    },
                                    'edge_type': {
                                        'S': '_change_'
                                    }
                                }
                            },
                            'assimilated': {
                                'BOOL': True
                            },
                            'potential_vertex': {
                                'M': {
                                    'sid_value': {
                                        'S': '649'
                                    },
                                    'internal_id': {
                                        'S': '6b6071c7958baca0fb44215560452e9c'
                                    },
                                    'object_type': {
                                        'S': 'ChangeLogEntry'
                                    },
                                    'identifier_stem': {
                                        'S': "['id_source', 'id_type', 'id_name']"
                                    },
                                    'object_properties': {
                                        'M': {
                                            'change_date_utc': {
                                                'NULL': True
                                            },
                                            'change_description': {
                                                'NULL': True
                                            },
                                            'change_date': {
                                                'NULL': True
                                            },
                                            'id_source': {
                                                'S': 'MBI'
                                            },
                                            'id_type': {
                                                'NULL': True
                                            },
                                            'changelog_id': {
                                                'N': '649'
                                            },
                                            'id_name': {
                                                'NULL': True
                                            }
                                        }
                                    },
                                    'id_value': {
                                        'N': '649'
                                    }
                                }
                            }
                        }
                    },
                    '_changed_': {
                        'M': {
                            'rule_entry': {
                                'M': {
                                    'if_absent': {
                                        'S': 'create'
                                    },
                                    'target_constants': {
                                        'L': [{
                                            'M': {
                                                'constant_name': {
                                                    'S': 'id_source'
                                                },
                                                'constant_value': {
                                                    'S': 'source.id_source'
                                                }
                                            }
                                        }
                                        ]
                                    },
                                    'target_specifiers': {
                                        'L': [{
                                            'M': {
                                                'specifier_type': {
                                                    'S': 'function'
                                                },
                                                'shared_properties': {
                                                    'NULL': True
                                                },
                                                'extracted_properties': {
                                                    'L': [{
                                                        'S': 'id_source'
                                                    }, {
                                                        'S': 'id_type'
                                                    }, {
                                                        'S': 'id_name'
                                                    }, {
                                                        'S': 'id_value'
                                                    }
                                                    ]
                                                },
                                                'function_name': {
                                                    'S': 'derive_change_targeted'
                                                },
                                                'specifier_name': {
                                                    'S': 'changed_target'
                                                }
                                            }
                                        }
                                        ]
                                    },
                                    'inbound': {
                                        'BOOL': False
                                    },
                                    'target_type': {
                                        'S': 'ExternalId'
                                    },
                                    'edge_type': {
                                        'S': '_changed_'
                                    }
                                }
                            },
                            'assimilated': {
                                'BOOL': False
                            },
                            'potential_vertex': {
                                'M': {
                                    'sid_value': {
                                        'S': '28'
                                    },
                                    'internal_id': {
                                        'S': '14671e379b32b7c875e28b0440012389'
                                    },
                                    'object_type': {
                                        'S': 'ExternalId'
                                    },
                                    'identifier_stem': {
                                        'S': '#vertex#ExternalId#{"id_source": "MBI", "id_type": "ExportBuilder", "id_name": "exportbuilder_id"}#'
                                    },
                                    'object_properties': {
                                        'M': {
                                            'id_source': {
                                                'S': 'MBI'
                                            },
                                            'id_type': {
                                                'S': 'ExportBuilder'
                                            },
                                            'id_value': {
                                                'N': '28'
                                            },
                                            'id_name': {
                                                'S': 'exportbuilder_id'
                                            }
                                        }
                                    },
                                    'id_value': {
                                        'N': '28'
                                    }
                                }
                            }
                        }
                    }
                }
            },
            'internal_id': {
                'S': 'a9e4709ba338250b0a55485ba373979f'
            },
            'object_type': {
                'S': 'Change'
            },
            'identifier_stem': {
                'S': '#vertex#Change#{"id_source": "MBI", "id_type": "ChangeLogDetail", "id_name": "changelogdetail_id"}#'
            },
            'completed': {
                'BOOL': False
            },
            'last_time_seen': {
                'N': '1541781738.447886943817138671875'
            },
            'extracted_data': {
                'S': '{"source": {"id_name": "changelogdetail_id", "id_type": "ChangeLogDetail", "id_source": "MBI", "detail_id": 1246, "changelog_id": 649, "data_dict_id": "", "detail_one_value": "", "detail_one": "", "detail_two": ""}, "changed_target": [{"change_date_utc": {"_alg_class": "datetime", "value": 1406685942.947}, "client_id": "", "clientvisit_id": "", "emp_id": 3796, "record_id": 28, "record_type": "ExportBuilder", "primarykey_name": "exportbuilder_id"}]}'
            },
            'sid_value': {
                'S': '1246'
            },
            'disposition': {
                'S': 'working'
            },
            'last_stage_seen': {
                'S': 'transformation'
            },
            'progress': {
                'M': {
                    'extraction': {
                        'N': '1541781737.7540950775146484375'
                    },
                    'monitoring': {
                        'N': '1541781669.0441429615020751953125'
                    },
                    'transformation': {
                        'N': '1541781738.447886943817138671875'
                    },
                    'assimilation': {
                        'M': {}
                    }
                }
            },
            'object_properties': {
                'M': {
                    'id_source': {
                        'S': 'MBI'
                    },
                    'detail_one': {
                        'NULL': True
                    },
                    'data_dict_id': {
                        'NULL': True
                    },
                    'id_type': {
                        'S': 'ChangeLogDetail'
                    },
                    'changelog_id': {
                        'N': '649'
                    },
                    'detail_id': {
                        'N': '1246'
                    },
                    'id_name': {
                        'S': 'changelogdetail_id'
                    },
                    'detail_two': {
                        'NULL': True
                    },
                    'detail_one_value': {
                        'NULL': True
                    }
                }
            },
            'id_value': {
                'N': '1246'
            }
        },
        'SequenceNumber': '73537100000000009063399183',
        'SizeBytes': 5401,
        'StreamViewType': 'NEW_AND_OLD_IMAGES'
    },
    'eventSourceARN': 'arn:aws:dynamodb:us-east-1:803040539655:table/GraphObjects/stream/2018-10-26T16:35:36.212'
}
