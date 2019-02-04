import pytest

from toll_booth.alg_obj.posts.accountant.supervisory_tree import SupervisorTree, SupervisionBranch


@pytest.mark.supervisor_tree
class TestSupervisorTree:
    def test_supervisor_tree_construction(self):
        supervisor_tree = SupervisorTree()
        assert isinstance(supervisor_tree, SupervisorTree)
        assert hasattr(supervisor_tree, '_branches')

    def test_supervisor_branch_construction(self, branch_id):
        branch = SupervisionBranch(branch_id)
        assert isinstance(branch, SupervisionBranch)
        assert branch.branch_id == branch_id
        assert hasattr(branch, 'add_supervised_id')
        assert hasattr(branch, 'add_supervisor_id')
        assert branch.supervisor_ids == []
        assert branch.supervised_ids == []

    def test_supervisor_tree_add_branch(self, branch_data):
        tree = SupervisorTree()
        branch = SupervisionBranch(*branch_data)
        tree.add_branch(branch)
        assert branch.branch_id in tree
        for id_value in branch.supervisor_ids:
            assert id_value in tree
            supervisor_branch = tree[id_value]
            assert isinstance(supervisor_branch, SupervisionBranch)
            assert supervisor_branch.branch_id == id_value
            assert supervisor_branch.supervised_ids == [branch.branch_id]
            assert supervisor_branch.supervisor_ids == []
        for id_value in branch.supervised_ids:
            assert id_value in tree
            supervised_branch = tree[id_value]
            assert isinstance(supervised_branch, SupervisionBranch)
            assert supervised_branch.branch_id == id_value
            assert supervised_branch.supervisor_ids == [branch.branch_id]
            assert supervised_branch.supervised_ids == []

    def test_get_parents(self, branch_data):
        tree = SupervisorTree()
        branch = SupervisionBranch(*branch_data)
        tree.add_branch(branch)
        parents = tree.get_parents(branch.branch_id)
        assert parents == branch.supervisor_ids
        for supervised_id in branch.supervised_ids:
            expected_parents = [branch.branch_id]
            expected_parents.extend(branch.supervisor_ids)
            parents = tree.get_parents(supervised_id)
            assert parents == expected_parents
