import pytest

from toll_booth.alg_obj.forge.fungi import Spore


@pytest.mark.usefixtures('mock_schema')
class TestShrooms:
    @pytest.mark.propagate
    def test_propagation(self, propagated_identifier_stem):
        spore = Spore(**propagated_identifier_stem)
        results = spore.propagate()
        print(results)
