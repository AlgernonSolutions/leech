import pytest

from toll_booth.alg_tasks.extractors import credible_ws


@pytest.mark.credible_ws
class TestCredibleWS:
    def test_get_index_max_min(self, credible_ws_payload):
        results = credible_ws.handler(credible_ws_payload, [])
        assert isinstance(results['max'], int)
        assert isinstance(results['min'], int)
        assert results['max'] >= results['min']
