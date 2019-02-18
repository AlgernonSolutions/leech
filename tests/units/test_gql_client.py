import pytest

from toll_booth.alg_tasks.toll_booth import aphid


@pytest.mark.gql_client
@pytest.mark.usefixtures('silence_x_ray')
class TestGqlClient:
    def test_get_vertex(self, mock_context):
        first_event = {
            'source': {
                'internal_id': '84a3b5c37a72a389a9c6bf401b5dd7b5',
                'vertex_type': 'ExternalId',
                '__typename': 'Vertex',
                'vertex_properties': [
                    {
                        '__typename': 'ObjectProperty',
                        'property_name': 'id_source',
                        'property_value': {
                            '__typename': 'ObjectPropertyValue',
                            'data_type': 'S',
                            'property_value': 'MBI'
                        }
                    },
                    {
                        '__typename': 'ObjectProperty',
                        'property_name': 'id_type',
                        'property_value': {
                            '__typename': 'ObjectPropertyValue',
                            'data_type': 'S',
                            'property_value': 'Employees'
                        }
                    },
                    {
                        '__typename': 'ObjectProperty',
                        'property_name': 'id_value',
                        'property_value': {
                            '__typename': 'ObjectPropertyValue',
                            'data_type': 'I',
                            'property_value': 3883
                        }
                    },
                    {
                        '__typename': 'ObjectProperty',
                        'property_name': 'id_name',
                        'property_value': {
                            '__typename': 'ObjectPropertyValue',
                            'data_type': 'S',
                            'property_value': 'emp_id'
                        }
                    }
                ]
            },
            'context': {},
            'object_type': 'Vertex',
            'object_property': 'ConnectedEdges',
            'username': 'AIDAJ6FECTLRA3JWBFVLU'
        }
        results = aphid(first_event, mock_context)
        second_event = {
            'source': None,
            'context': {
                'token':  results['page_info']['token']
            },
            'object_type': 'EdgeConnection',
            'username': 'AIDAJ6FECTLRA3JWBFVLU'
        }
        second_results = aphid(second_event, mock_context)
        third_event = {
            'source': None,
            'context': {
                'token': second_results['page_info']['token']
            },
            'object_type': 'EdgeConnection',
            'username': 'AIDAJ6FECTLRA3JWBFVLU'
        }
        third_results = aphid(third_event, mock_context)
        print(second_event)
