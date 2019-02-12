import logging

from admin.set_logging import set_logging
from toll_booth.alg_obj.aws.trident.trident_manager import TridentManager


def create_db_instance(cluster_id, db_identifier, instance_class, db_param_group_name):
    manager = TridentManager()
    create_results = manager.create_db_instance(
        cluster_id=cluster_id,
        db_identifier=db_identifier,
        instance_class=instance_class,
        db_parameter_group_name=db_param_group_name
    )
    return create_results


if __name__ == '__main__':
    set_logging()
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
    logging.info(f'started the creation of a Neptune graph instance: {cluster_args}')
    instance_args = (new_cluster_id, new_db_identifier, new_instance_size, assigned_db_param_group_name)
    results = create_db_instance(*instance_args)
    logging.info(f'completed the creation call of a Neptune graph instance: {cluster_args}')
