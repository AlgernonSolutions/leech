import pytest

from toll_booth.alg_obj.graph.ogm.ogm import Ogm


@pytest.mark.ogm
class TestOgm:
    def test_graph_object(self):
        ogm = Ogm()
