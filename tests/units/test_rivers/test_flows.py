import pytest

from toll_booth.alg_tasks.rivers.flows.fungus import fungus
from toll_booth.alg_tasks.rivers.flows.leech.fungal_leech import fungal_leech


@pytest.mark.flows
@pytest.mark.usefixtures('silence_x_ray')
class TestFlows:
    def test_fungal_leech(self, mock_context):
        fungal_leech()
        print()
