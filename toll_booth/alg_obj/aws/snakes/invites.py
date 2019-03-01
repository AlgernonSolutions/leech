import boto3
from botocore.exceptions import ClientError


class ObjectDownloadLink:
    def __init__(self, bucket_name, remote_file_path, expiration_seconds=172800, local_file_path=None):
        self._bucket_name = bucket_name
        self._remote_file_path = remote_file_path
        self._expiration_seconds = expiration_seconds
        self._local_file_path = local_file_path
        self._stored = False

    def _store(self):
        s3 = boto3.resource('s3')
        if self._local_file_path is None:
            raise RuntimeError(f'tried to generate a download url for {self._remote_file_path}, but it does not exist remotely, and no local path was provided')
        s3.Bucket(self._bucket_name).upload_file(self._local_file_path, self._local_file_path)
        self._stored = True

    def _check(self):
        if self._stored is True:
            return True
        resource = boto3.resource('s3')
        object_resource = resource.Object(self._bucket_name, self._remote_file_path)
        try:
            object_resource.load()
        except ClientError as e:
            return int(e.response['Error']['Code']) != 404
        return True

    def __str__(self):
        client = boto3.client('s3')
        if not self._check():
            self._store()
        pre_signed = client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': self._bucket_name,
                'Key': self._remote_file_path
            },
            ExpiresIn=self._expiration_seconds
        )
        return pre_signed
