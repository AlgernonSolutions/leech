import boto3


class DbParameter:
    def __init__(self, parameter_name, parameter_value, description):
        self._parameter_name = parameter_name
        self._parameter_value = parameter_value
        self._description = description

    @property
    def parameter_name(self):
        return self._parameter_name

    @property
    def parameter_value(self):
        return self._parameter_value

    @property
    def description(self):
        return self._description


class DbSubnet:
    def __init__(self, vpc_id, subnet_identifier, subnet_az):
        self._vpc_id = vpc_id
        self._subnet_identifier = subnet_identifier
        self._subnet_az = subnet_az

    @property
    def vpc_id(self):
        return self._vpc_id

    @property
    def subnet_identifier(self):
        return self._subnet_identifier

    @property
    def subnet_az(self):
        return self._subnet_az


class ClusterParameterGroup:
    def __init__(self, group_name, group_family, group_description, db_parameters=None, tags=None):
        if not tags:
            tags = []
        if not db_parameters:
            db_parameters = []
        self._group_name = group_name
        self._group_family = group_family
        self._group_description = group_description
        self._db_parameters = db_parameters
        self._tags = tags

    @property
    def group_name(self):
        return self._group_name

    @property
    def group_family(self):
        return self._group_family

    @property
    def group_description(self):
        return self._group_description

    @property
    def db_parameters(self):
        return self._db_parameters

    def create(self):
        client = boto3.client('neptune')
        group_args = {
            'DBParameterGroupName': self._group_name,
            'DBParameterGroupFamily': self._group_family,
        }
        if self._group_description:
            group_args['Description'] = self._group_description
        response = client.create_db_cluster_parameter_group(**group_args)
        return response


class DbParameterGroup:
    def __init__(self, group_name, group_family, group_description, db_parameters=None, for_db=False, tags=None):
        if not tags:
            tags = []
        if not db_parameters:
            db_parameters = []
        self._group_name = group_name
        self._group_family = group_family
        self._group_description = group_description
        self._db_parameters = db_parameters
        self._for_db = for_db
        self._tags = tags

    @property
    def for_db(self):
        return self._for_db

    @property
    def group_name(self):
        return self._group_name

    @property
    def group_family(self):
        return self._group_family

    @property
    def group_description(self):
        return self._group_description

    def create(self):
        client = boto3.client('neptune')
        group_args = {
            'DBParameterGroupName': self._group_name,
            'DBParameterGroupFamily': self._group_family,
        }
        if self._group_description:
            group_args['Description'] = self._group_description
        response = client.create_db_parameter_group(**group_args)
        return response


class SubnetGroup:
    def __init__(self, group_name, group_description, subnets, tags=None):
        if not tags:
            tags = []
        self._group_name = group_name
        self._group_description = group_description
        self._subnets = subnets
        self._tags = tags

    @property
    def group_name(self):
        return self._group_name

    @property
    def group_description(self):
        return self._group_description

    @property
    def subnets(self):
        return self._subnets


class GraphDbInstance:
    def __init__(self, cluster_id, db_identifier, instance_class, db_parameter_group_name, **kwargs):
        self._cluster_id = cluster_id
        self._db_identifier = db_identifier
        self._db_name = kwargs.get('db_name')
        self._instance_class = instance_class
        self._db_parameter_group_name = db_parameter_group_name
        self._availability_zone = kwargs.get('availability_zone')
        self._monitoring_interval = kwargs.get('monitoring_interval', 0)
        self._monitoring_role_arn = kwargs.get('monitoring_role_arn')

    @property
    def instance_id(self):
        return self._db_identifier

    def create(self, **kwargs):
        client = kwargs.get('client')
        if not client:
            client = boto3.client('neptune')
        instance_args = {
            'DBClusterIdentifier': self._cluster_id,
            'DBInstanceIdentifier': self._db_identifier,
            'DBInstanceClass': self._instance_class,
            'Engine': 'neptune',
            'DBParameterGroupName': self._db_parameter_group_name,
            'MonitoringInterval': self._monitoring_interval

        }
        if self._db_name:
            instance_args['DBName'] = self._db_name
        if self._monitoring_interval:
            instance_args['MonitoringRoleArn'] = self._monitoring_role_arn
        if self._availability_zone:
            instance_args['AvailabilityZone'] = self._availability_zone
        results = client.create_db_instance(**instance_args)
        return results


class GraphDbCluster:
    def __init__(self, cluster_id, **kwargs):
        self._cluster_id = cluster_id
        self._az_zones = kwargs.get('az_zones', ['us-east-1a'])
        self._vpc_security_group_ids = kwargs.get('vpc_security_group_ids', [])
        self._engine_version = kwargs.get('engine_version')
        self._port = kwargs.get('port', 8182)
        self._storage_encrypted = kwargs.get('storage_encrypted', True)
        self._iam_authentication = kwargs.get('iam_authentication', True)
        self._backup_retention = kwargs.get('backup_retention', 30)
        self._db_name = kwargs.get('db_name')
        self._cluster_parameter_group_name = kwargs.get('cluster_parameter_group_name')
        self._subnet_group_name = kwargs.get('subnet_group_name')
        self._kms_id = kwargs.get('kms_id', None)
        self._instances = kwargs.get('db_instances', [])

    @property
    def cluster_id(self):
        return self._cluster_id

    @property
    def instances(self):
        return self._instances

    def __iter__(self):
        return iter(self._instances)

    def create(self, client=None):
        if not client:
            client = boto3.client('neptune')

        cluster_args = {
            'AvailabilityZones': self._az_zones,
            'BackupRetentionPeriod': self._backup_retention,
            'DBClusterIdentifier': self._cluster_id,
            'VpcSecurityGroupIds': self._vpc_security_group_ids,
            'Engine': 'neptune',
            'Port': self._port,
            'StorageEncrypted': self._storage_encrypted,
            'DBSubnetGroupName': self._subnet_group_name,
            'DBClusterParameterGroupName': self._cluster_parameter_group_name,
            'EnableIAMDatabaseAuthentication': self._iam_authentication
        }
        if self._storage_encrypted:
            if self._kms_id:
                cluster_args['KmsKeyId'] = self._kms_id
        response = client.create_db_cluster(**cluster_args)
        return response


class ClusterSnapShot:
    def __init__(self, cluster_id, snapshot_id, snapshot_create_time, aws_snapshot_data):
        self._cluster_id = cluster_id
        self._snapshot_id = snapshot_id
        self._snapshot_create_time = snapshot_create_time
        self._aws_snapshot_data = aws_snapshot_data

    @property
    def snapshot_id(self):
        return self._snapshot_id

    @property
    def aws_snapshot_data(self):
        return self._aws_snapshot_data

    @property
    def status(self):
        return self._aws_snapshot_data['Status']

    @property
    def snapshot_create_time(self):
        return self._snapshot_create_time

    @classmethod
    def create(cls, cluster_id, **kwargs):
        from datetime import datetime
        client = kwargs.get('client')
        if not client:
            client = boto3.client('neptune')
        snapshot_time = datetime.utcnow()
        snapshot_id = f'{cluster_id}-{snapshot_time.timestamp()}'
        snapshot_id = snapshot_id.replace('.', '-')
        response = client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snapshot_id,
            DBClusterIdentifier=cluster_id
        )
        snapshot_data = response['DBClusterSnapshot']
        return cls(cluster_id, snapshot_id, snapshot_data['SnapshotCreateTime'], snapshot_data)

    def restore_cluster(self, **kwargs):
        client = kwargs.get('client')
        if not client:
            client = boto3.client('neptune')
        client.restore_db_cluster_from_snapshot(**self._aws_snapshot_data)
