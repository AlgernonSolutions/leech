import boto3


class ObjectDownloadLink:
    def __init__(self, bucket_name, file_path):
        self._bucket_name = bucket_name
        self._file_path = file_path

    def __str__(self):
        client = boto3.client('s3')
        one_minute = 60
        one_hour = one_minute * 60
        one_day = one_hour * 24
        pre_signed = client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': self._bucket_name,
                'Key': self._file_path
            },
            ExpiresIn=one_day
        )
        return pre_signed
