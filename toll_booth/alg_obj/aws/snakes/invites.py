import boto3


class ObjectDownloadLink:
    def __init__(self, bucket_name, file_path, expiration_seconds=86400):
        self._bucket_name = bucket_name
        self._file_path = file_path
        self._expiration_seconds = expiration_seconds

    def __str__(self):
        client = boto3.client('s3')
        pre_signed = client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': self._bucket_name,
                'Key': self._file_path
            },
            ExpiresIn=self._expiration_seconds
        )
        return pre_signed
