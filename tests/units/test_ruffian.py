import pytest

from toll_booth.alg_tasks.ruffians import decide, labor


@pytest.mark.usefixtures('silence_x_ray')
class TestRuffian:
    @pytest.mark.ruffian_labor
    def test_ruffian_labor(self, mock_context):
        event = {
          "work_lists": {
            "list_name": "credible",
            "number_threads": 1,
            "is_vpc": False
          },
          "domain_name": "TheLeech"
        }
        labor(event, mock_context)
