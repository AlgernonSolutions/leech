import re

import pytest

from toll_booth.alg_obj.graph.ogm.generator import CommandGenerator
from toll_booth.alg_obj.graph.ogm.ogm import Ogm
from toll_booth.alg_tasks.metl_tasks.load import load


@pytest.mark.ogm
class TestOgm:
    def test_load(self, load_order):
        results = load(load_order['task_args'], context=[])

    def test_graph_object(self):
        ogm = Ogm()


@pytest.mark.command_generator
class TestCommandGenerator:
    @classmethod
    def _verify_command(cls, graph_command):
        reg_str = r"g\.(V|E)\(('[a-f0-9]{32}')\)\.fold\(\)\.coalesce\(unfold\(\), add\1\('(?!(None))\w+(::stub)*'\)(" \
                  r"\.from\(g\.V\(('[a-f0-9]{32}')\)\)\.to\(g\.V\(('[a-f0-9]{32}')\)\))*((\.property\()((('[\w]+', " \
                  r")((\d+)|('[\w\s\-\:\.\+]*')))|(id, \2))\))+\)"
        test_reg = re.compile(reg_str)
        match_test = test_reg.match(graph_command)
        return match_test is not None

    def test_vertex_command_generation(self, potential_vertex):
        generator = CommandGenerator.get_for_obj_type(potential_vertex.object_type)
        test_command = generator.create_command(potential_vertex)
        assert self._verify_command(test_command) is True
