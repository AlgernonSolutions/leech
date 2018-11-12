import pytest

from toll_booth.alg_obj.forge.snakes import MonitorSnake


@pytest.mark.snakes
class TestSnake:
    def test_snake(self, vd_identifier_stem):
        snake = MonitorSnake(vd_identifier_stem)
        snake.monitor()
