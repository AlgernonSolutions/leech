import pytest

from toll_booth.alg_obj.forge.fungi import Spore


@pytest.mark.snakes
class TestSnake:
    def test_snake(self, vd_identifier_stem):
        snake = Spore(vd_identifier_stem)
        snake.monitor()
