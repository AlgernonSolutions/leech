from toll_booth.alg_obj.aws.trident.trident_manager import TridentManager


def create_cluster(cluster_id, instance_identifier, cluster_param_group_name, subnet_group_name,  vpc_security_group_ids):
    manager = TridentManager()
    create_results = manager.create_db_cluster(
        cluster_id=cluster_id,
        instance_identifier=instance_identifier,
        subnet_group_name=subnet_group_name,
        cluster_parameter_group_name=cluster_param_group_name,
        vpc_security_group_ids=vpc_security_group_ids
    )
    return create_results


if __name__ == '__main__':
    new_cluster_id = 'algernon'
    new_db_identifier = 'algernon-1'
    new_instance_size = 'db.r4.large'
    assigned_db_param_group_name = 'alg-neptune-params'
    assigned_cluster_param_group_name = 'alg-neptune-cluster-params'
    assigned_subnet_group_name = 'neptune_sng'
    assigned_vpc_security_group_ids = ['sg-cea73ab9']
    cluster_args = (
        new_cluster_id, new_db_identifier, assigned_cluster_param_group_name,
        assigned_subnet_group_name, assigned_vpc_security_group_ids
    )
    instance_args = (new_cluster_id, new_db_identifier, new_instance_size, assigned_db_param_group_name)
    create_cluster(*cluster_args)
