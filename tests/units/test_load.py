import pytest

from toll_booth.alg_tasks.toll_booth import work


@pytest.mark.test_load
@pytest.mark.usefixtures('silence_x_ray')
class TestLoad:
    def test_load(self, load_task, mock_context, mock_neptune):
        results = work(load_task, mock_context)
        mock_neptune.assert_called_once()
        print()
