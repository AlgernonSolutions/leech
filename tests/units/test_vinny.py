import pytest

from toll_booth.alg_obj.graph.ogm.santorini import Vinny


@pytest.mark.test_vinny
class TestVinny:
    def test_identifies_new_vertex(self, assimilated_vertex_stream_event):
        vinny = Vinny()
        results = vinny.rig_explosion(assimilated_vertex_stream_event)
        for result in results:
            assert 'graphing' in result
