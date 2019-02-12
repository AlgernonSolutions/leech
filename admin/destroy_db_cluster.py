from toll_booth.alg_obj.aws.trident.trident_manager import TridentManager

if __name__ == '__main__':
    manager = TridentManager()
    db_clusters = manager.get_db_clusters('algernon')
    for cluster in db_clusters:
        for instance in cluster:
            manager.destroy_db_instance(instance.instance_id)
