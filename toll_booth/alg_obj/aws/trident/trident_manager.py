from datetime import datetime

import boto3

from toll_booth.alg_obj.aws.trident.trident_objects import DbParameter, GraphDbInstance, \
    DbParameterGroup, DbSubnet, SubnetGroup, GraphDbCluster, ClusterSnapShot


class TridentManager:
    def __init__(self):
        self._client = boto3.client('neptune')

    @property
    def client(self):
        return self._client

    def get_parameter_groups(self, group_name=None, for_db=False):
        parameter_groups = []
        args = {}
        name_filter = 'DBClusterParameterGroupName'
        group_filter = 'DBClusterParameterGroups'
        group_name_filter = 'DBClusterParameterGroupName'
        if for_db:
            name_filter = 'DBParameterGroupName'
            group_filter = 'DBParameterGroups'
            group_name_filter = 'DBParameterGroupName'
        if group_name:
            args[name_filter] = group_name
        if for_db:
            response = self._client.describe_db_parameter_groups(**args)
        else:
            response = self._client.describe_db_cluster_parameter_groups(**args)
        for entry in response[group_filter]:
            if entry['DBParameterGroupFamily'] != 'neptune1':
                continue
            group_name = entry[group_name_filter]
            parameters = self.get_parameters(group_name, for_db=for_db)
            new_group = DbParameterGroup(
                group_name, entry['DBParameterGroupFamily'], entry['Description'], parameters, for_db
            )
            parameter_groups.append(new_group)
        return parameter_groups

    def get_parameters(self, parameter_group_name, for_db=False):
        parameters = []
        args = {}
        name_filter = 'DBClusterParameterGroupName'
        if for_db:
            name_filter = 'DBParameterGroupName'
        if parameter_group_name:
            args[name_filter] = parameter_group_name
        if for_db:
            results = self._client.describe_db_parameters(**args)
        else:
            results = self._client.describe_db_cluster_parameters(**args)
        for entry in results['Parameters']:
            parameter = DbParameter(entry['ParameterName'], entry['ParameterValue'], entry['Description'])
            parameters.append(parameter)
        return parameters

    def get_subnet_groups(self, subnet_group_name):
        groups = []
        response = self._client.describe_db_subnet_groups(DBSubnetGroupName=subnet_group_name)
        subnet_groups = response['DBSubnetGroups']
        for entry in subnet_groups:
            subnets = []
            group_name = entry['DBSubnetGroupName']
            description = entry['DBSubnetGroupDescription']
            for subnet_entry in entry['Subnets']:
                subnet = DbSubnet(
                    entry['VpcId'], subnet_entry['SubnetIdentifier'], subnet_entry['SubnetAvailabilityZone']['Name'])
                subnets.append(subnet)
            subnet_group = SubnetGroup(group_name, description, subnets)
            groups.append(subnet_group)
        return groups

    def get_db_instances(self, instance_identifier=None):
        instances = []
        args = {}
        if instance_identifier:
            args['DBInstanceIdentifier'] = instance_identifier
        response = self._client.describe_db_instances(**args)
        for entry in response['DBInstances']:
            db_parameter_group_names = [x['DBParameterGroupName'] for x in entry['DBParameterGroups']]
            instance_args = {
                'cluster_id': entry['DBClusterIdentifier'],
                'db_identifier': entry['DBInstanceIdentifier'],
                'instance_class': entry['DBInstanceClass'],
                'availability_zone': entry['AvailabilityZone'],
                'db_parameter_group_name': db_parameter_group_names[0]
            }
            instance = GraphDbInstance(**instance_args)
            instances.append(instance)
        return instances

    def get_db_clusters(self, cluster_name=None):
        clusters = []
        args = {}
        if cluster_name:
            args['DBClusterIdentifier'] = cluster_name
        response = self._client.describe_db_clusters(**args)
        for entry in response['DBClusters']:
            cluster_id = entry['DBClusterIdentifier']
            all_instances = []
            db_instances = [self.get_db_instances(x['DBInstanceIdentifier']) for x in entry['DBClusterMembers']]
            for db_instance in db_instances:
                all_instances.extend(db_instance)
            cluster_args = {
                'az_zones': entry['AvailabilityZones'],
                'vpc_security_group_ids': [x['VpcSecurityGroupId'] for x in entry['VpcSecurityGroups']],
                'engine_version': entry['EngineVersion'],
                'port': entry['Port'],
                'multi_az': entry['MultiAZ'],
                'storage_encrypted': entry['StorageEncrypted'],
                'kms_id': entry['KmsKeyId'],
                'iam_authentication': entry['IAMDatabaseAuthenticationEnabled'],
                'db_instances': all_instances
            }
            db_cluster = GraphDbCluster(cluster_id, **cluster_args)
            clusters.append(db_cluster)
        return clusters

    def create_db_instance(self, cluster_id, db_identifier, instance_class, db_parameter_group_name, **kwargs):
        instance = GraphDbInstance(cluster_id, db_identifier, instance_class, db_parameter_group_name, **kwargs)
        create_results = instance.create(client=self._client)
        return create_results

    def create_db_cluster(self, cluster_id, **kwargs):
        cluster = GraphDbCluster(cluster_id, **kwargs)
        create_results = cluster.create(client=self._client)
        return create_results

    def take_cluster_snapshot(self, cluster_id):
        snapshot = ClusterSnapShot.create(cluster_id, client=self._client)
        return snapshot

    def check_snapshot_status(self, snapshot_id):
        snapshot = self.retrieve_cluster_snapshots(snapshot_id=snapshot_id)
        return snapshot.status

    def retrieve_cluster_snapshots(self, cluster_id=None, snapshot_id=None):
        snapshots = []
        args = {}
        if cluster_id:
            args['DBClusterIdentifier '] = cluster_id
        if snapshot_id:
            args['DBClusterSnapshotIdentifier '] = snapshot_id
        paginator = self._client.get_paginator('describe_db_cluster_snapshots')
        iterator = paginator.paginate(**args)
        for page in iterator:
            for entry in page['DBClusterSnapshots']:
                snapshot = ClusterSnapShot(
                    entry['DBClusterIdentifier'], entry['DBClusterSnapshotIdentifier'],
                    entry['SnapshotCreateTime'], entry
                )
                snapshots.append(snapshot)
        if snapshot_id:
            for snapshot in snapshots:
                return snapshot
        return snapshots

    def restore_cluster_snapshot(self, snapshot_id):
        snapshot = self.retrieve_cluster_snapshots(snapshot_id=snapshot_id)
        aws_field_names = [
            'AvailabilityZones', 'DBClusterIdentifier', 'SnapshotIdentifier',
            'Engine', 'EngineVersion', 'Port', 'DBSubnetGroupName', 'DatabaseName',
            'OptionGroupName', 'VpcSecurityGroupIds', 'Tags', 'KmsKeyId', 'EnableIAMDatabaseAuthentication'
        ]
        snapshot_args = {x: snapshot.aws_snapshot_data[x] for x in aws_field_names}
        response = self._client.restore_db_cluster_from_snapshot(**snapshot_args)
        return response

    def restore_cluster_from_most_recent_snapshot(self, cluster_id):
        snapshots = self.retrieve_cluster_snapshots(cluster_id=cluster_id)
        snapshots = sorted(snapshots, key=lambda x: x.snapshot_create_time, reverse=True)
        self.restore_cluster_snapshot(snapshots[0].snapshot_id)

    def destroy_db_instance(self, instance_id):
        response = self._client.delete_db_instance(DBInstanceIdentifier=instance_id)
        return response

    def destroy_db_cluster(self, cluster_id):
        snapshot_time = datetime.utcnow()
        snapshot_id = f'{cluster_id}-{snapshot_time.timestamp()}'
        snapshot_id = snapshot_id.replace('.', '-')
        response = self._client.delete_db_cluster(
            DBClusterIdentifier=cluster_id,
            FinalDBSnapshotIdentifier=snapshot_id
        )
        return response
