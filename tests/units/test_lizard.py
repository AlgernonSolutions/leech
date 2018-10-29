from mock import patch

from tests.steps.actor_setup import patches
from toll_booth.alg_obj.forge.lizards import MonitorLizard

identifier_stem = '#vertex#ExternalId#{\"id_source\": \"MBI\", \"id_type\": \"Employees\", \"id_name\": \"emp_id\"}#'


class TestLizard:
    def test_lizard(self):
        with patch(patches.x_ray_patch_begin), patch(patches.x_ray_patch_end):
            lizard = MonitorLizard(identifier_stem=identifier_stem)
            lizard.monitor()
