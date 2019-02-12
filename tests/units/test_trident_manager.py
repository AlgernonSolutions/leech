import pytest

from toll_booth.alg_obj.aws.trident.trident_manager import TridentManager
from toll_booth.alg_obj.aws.trident.trident_objects import SubnetGroup, DbParameterGroup, GraphDbCluster, \
    GraphDbInstance


@pytest.mark.trident_manager
class TestTridentManager:
    def test_trident_manager(self):
        trident_manager = TridentManager()
        assert isinstance(trident_manager, TridentManager)

    def test_get_cluster_parameter_groups(self):
        manager = TridentManager()
        parameter_groups = manager.get_parameter_groups()
        assert parameter_groups
        for entry in parameter_groups:
            assert isinstance(entry, DbParameterGroup)
            assert entry.for_db is False

    def test_get_db_parameter_groups(self):
        manager = TridentManager()
        parameter_groups = manager.get_parameter_groups(for_db=True)
        assert parameter_groups
        for entry in parameter_groups:
            assert isinstance(entry, DbParameterGroup)
            assert entry.for_db is True

    def test_get_db_clusters(self):
        cluster_name = 'test-cluster'
        manager = TridentManager()
        clusters = manager.get_db_clusters(cluster_name)
        assert clusters
        for cluster in clusters:
            assert isinstance(cluster, GraphDbCluster)

    def test_get_db_instances(self):
        manager = TridentManager()
        instances = manager.get_db_instances()
        assert instances
        for instance in instances:
            assert isinstance(instance, GraphDbInstance)


