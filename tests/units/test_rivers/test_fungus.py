import pytest

from toll_booth.alg_tasks.rivers.flows.fungus import fungus


@pytest.mark.test_fungus
@pytest.mark.usefixtures('silence_x_ray')
class TestFungus:
    def test_fungus(self):
        print()
