import pytest

from toll_booth.alg_tasks.rivers.flows.fungus import fungus


@pytest.mark.flows
@pytest.mark.usefixtures('silence_x_ray')
class TestFlows:
    def test_fungus(self, mock_context):
        fungus('1')
        print()
