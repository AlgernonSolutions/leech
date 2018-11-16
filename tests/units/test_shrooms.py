import pytest

from toll_booth.alg_obj.forge.fungi import Spore


@pytest.mark.usefixtures('mock_schema')
class TestShrooms:
    @pytest.mark.propagate
    def test_propagation(self, monitored_ext_id_identifier_stem):
        spore = Spore(identifier_stem=monitored_ext_id_identifier_stem)
        results = spore.propagate()
        print(results)
