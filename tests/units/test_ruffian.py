import pytest

from toll_booth.alg_tasks.ruffians import decide, labor


@pytest.mark.usefixtures('silence_x_ray')
class TestRuffian:
    @pytest.mark.ruffian_labor
    def test_ruffian_labor(self, mock_context):
        event = {
          "decider_list": "f-4",
          "work_lists": {
            "f-4": 1,
            "credible": 1
          },
          "domain_name": "TheLeech"
        }
        labor(event, mock_context)
