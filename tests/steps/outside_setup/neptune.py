import json

from mock import MagicMock


class VertexResponse:
    def __init__(self, vertex_name, internal_id, vertex_properties=None):
        if not vertex_properties:
            vertex_properties = []
        self._vertex_name = vertex_name
        self._internal_id = internal_id
        self._vertex_properties = vertex_properties

    @classmethod
    def generic(cls):
        mock_properties = [
            MagicMock(property_name='some_mock_property', property_value='mock_property_value'),
            MagicMock(property_name='some_other_mock_property', property_value=2),
        ]
        return cls('mock_vertex', 'some_mock_internal_id', mock_properties)

    @property
    def internal_id(self):
        return self._internal_id

    @property
    def as_json(self):
        vertex_properties = {}
        for vertex_property in self._vertex_properties:
            property_name = vertex_property.property_name
            property_entry = {
                '@type': 'g:VertexProperty',
                '@value': {
                    'id': {
                        '@type': 'g:Int32',
                        '@value': -1831181351
                    },
                    'value': vertex_property.property_value,
                    'label': property_name
                }
            }
            vertex_properties[property_name] = [property_entry]

        return {
            '@type': 'g:Vertex',
            '@value': {
                'id': self._internal_id,
                'label': self._vertex_name,
                'properties': vertex_properties
            }
        }

    def __str__(self):
        return json.dumps(self.as_json)


generic_request_response = {
    'requestId': '1cb2f405-5d5f-275c-0b97-e636082f1b26',
    'status': {
        'message': '',
        'code': 200,
        'attributes': {
            '@type': 'g:Map',
            '@value': []
        }
    },
    'result': {
        'data': {
            '@type': 'g:List',
            '@value': [
                {
                    '@type': 'g:Vertex',
                    '@value': {
                        'id': '15124126dc7a5cd0deacf8bb617c3827',
                        'label': 'ExternalId',
                        'properties': {
                            'id_source': [
                                {
                                    '@type': 'g:VertexProperty',
                                    '@value': {
                                        'id': {
                                            '@type': 'g:Int32',
                                            '@value': -1831181351
                                        },
                                        'value': 'MBI',
                                        'label': 'id_source'
                                    }
                                }
                            ],
                            'id_type': [
                                {
                                    '@type': 'g:VertexProperty',
                                    '@value': {
                                        'id': {
                                            '@type': 'g:Int32',
                                            '@value': -103189433
                                        },
                                        'value': 'Employees',
                                        'label': 'id_type'
                                    }
                                }
                            ],
                            'id_name': [
                                {
                                    '@type': 'g:VertexProperty',
                                    '@value': {
                                        'id': {
                                            '@type': 'g:Int32',
                                            '@value': 211786859
                                        },
                                        'value': 'emp_id',
                                        'label': 'id_name'
                                    }
                                }
                            ],
                            'id_value': [
                                {
                                    '@type': 'g:VertexProperty',
                                    '@value': {
                                        'id': {
                                            '@type': 'g:Int32',
                                            '@value': 1576499060
                                        },
                                        'value': {
                                            '@type': 'g:Int32',
                                            '@value': 4647
                                        },
                                        'label': 'id_value'
                                    }
                                }
                            ]
                        }
                    }
                }
            ]
        },
        'meta': {
            '@type': 'g:Map',
            '@value': []
        }
    }
}


class MockNeptune:
    @classmethod
    def get_for_loader(cls, context):
        post_return = MagicMock(name='post_return')
        post_return.status_code = 200
        post_return.text = generic_request_response
        return post_return


def intercept_request(*args, **kwargs):
    query_params = kwargs['params']
    vertex = VertexResponse.generic()
    if 'g.E' in query_params:
        pass
    post_return = MagicMock(name='post_return')
    post_return.status_code = 200
    post_return.text = json.dumps({
        'requestId': '1cb2f405-5d5f-275c-0b97-e636082f1b26',
        'status': {
            'message': '',
            'code': 200,
            'attributes': {
                '@type': 'g:Map',
                '@value': []
            }
        },
        'result': {
            'data': {
                '@type': 'g:List',
                '@value': [vertex.as_json]
            },
            'meta': {
                '@type': 'g:Map',
                '@value': []
            }
        }
    })
    return post_return
