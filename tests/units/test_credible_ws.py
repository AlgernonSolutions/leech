from toll_booth.alg_tasks.extractors import credible_ws

payload = {
    'step_name': 'index_query',
    'step_args': {
        'id_source': 'MBI',
        'id_type': 'Employees',
        'id_name': 'emp_id',
        'graph_type': 'vertex',
        'object_type': 'ExternalId'
    }
}


class TestCredibleWS:
    def test_get_index_max_min(self):
        results = credible_ws.handler(payload, [])
        assert isinstance(results, int)
