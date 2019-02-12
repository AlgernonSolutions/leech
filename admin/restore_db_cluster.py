from toll_booth.alg_obj.aws.trident.trident_manager import TridentManager


if __name__ == '__main__':
    manager = TridentManager()
    manager.restore_cluster_from_most_recent_snapshot('algernon')
