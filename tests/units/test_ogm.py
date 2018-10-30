import pytest

from toll_booth.alg_obj.graph.ogm.ogm import Ogm
from toll_booth.alg_tasks.metl_tasks.load import load

graph_event = {
    'task_name': 'load',
    'task_args': {
        'keys': {
            'sid_value': {'S': '1002'},
            'identifier_stem': {
                'S': '#vertex#ExternalId#{"id_source": "MBI", "id_type": "Clients", "id_name": "client_id"}#'
            }
        },
        'table_name': 'GraphObjects'
    }
}


@pytest.mark.ogm
class TestOgm:
    @pytest.mark.parametrize('event', [graph_event])
    def test_load(self, event):
        results = load(event['task_args'], context=[])

    def test_graph_object(self):
        ogm = Ogm()
