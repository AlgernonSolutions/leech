import pytest

from toll_booth.alg_obj.forge.credible_specifics.change_types import ChangeTypes


@pytest.mark.change_types
class TestChangeTypes:
    def test_change_types(self):
        change_types = ChangeTypes.get()
