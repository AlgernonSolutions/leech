import pytest

from toll_booth.alg_obj.forge.credible_specifics.change_types import ChangeTypes


class TestChangeTypes:
    @pytest.mark.change_types
    def test_change_types(self):
        change_types = ChangeTypes.retrieve()
        print()
