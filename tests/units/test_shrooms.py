import pytest

from toll_booth.alg_obj.aws.sapper.leech_driver import LeechDriver
from toll_booth.alg_obj.forge.fungi import Spore, Shroom


@pytest.mark.usefixtures('mock_schema')
class TestShrooms:
    @pytest.mark.propagate
    def test_propagation(self, propagated_identifier_stem):
        spore = Spore(**propagated_identifier_stem)
        results = spore.propagate()
        print(results)

    @pytest.mark.fruit
    def test_fruiting(self, propagation_id, mock_context):
        shroom = Shroom(propagation_id, context=mock_context)
        results = shroom.fruit()
        print()

    @pytest.mark.get_changelog_types
    def test_get_changelog_types(self):
        driver = LeechDriver()
        changelog_types = driver.get_changelog_types()
        print()

    @pytest.mark.get_changelog_type_category
    def test_get_changelog_types(self):
        driver = LeechDriver()
        changelog_types = driver.get_changelog_types(category='Changes')
        print()

    @pytest.mark.get_changelog_type_names
    def test_get_changelog_types(self):
        driver = LeechDriver()
        changelog_types = driver.get_changelog_types(categories_only=True)
        print()