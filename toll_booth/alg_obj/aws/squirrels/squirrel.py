import json

import boto3
from botocore.exceptions import ClientError


class Opossum:
    @classmethod
    def get_database_secrets(cls):
        return cls.get_secrets('db_creds')

    @classmethod
    def get_wet_secrets(cls):
        return cls.get_secrets('neptune_creds')

    @classmethod
    def get_gql_secret(cls):
        return cls.get_secrets('gql')

    @classmethod
    def get_untrustworthy_export_key(cls, domain_name):
        secret_name = '%s_Export_Key' % domain_name
        secret = cls.get_secrets(secret_name)
        return secret['key_value']

    @classmethod
    def get_untrustworthy_credentials(cls, id_source):
        secret_name = '%s_Credible_Credentials' % id_source
        secret = cls.get_secrets(secret_name)
        return secret

    @classmethod
    def get_trident_user_key(cls):
        secret_name = 'Trident_User_Key'
        secret = cls.get_secrets(secret_name)
        return secret['trident_user_key_id'], secret['trident_user_key']

    @classmethod
    def get_secrets(cls, secret_name):
        secret_name = secret_name
        endpoint_url = "https://secretsmanager.us-east-1.amazonaws.com"
        region_name = "us-east-1"

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name,
            endpoint_url=endpoint_url
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print("The requested secret " + secret_name + " was not found")
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                print("The request was invalid due to:", e)
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                print("The request had invalid params:", e)
        else:
            # Decrypted secret using the associated KMS CMK
            # Depending on whether the secret was a string or binary, one of these fields will be populated
            if 'SecretString' in get_secret_value_response:
                return json.loads(get_secret_value_response['SecretString'])
            else:
                return get_secret_value_response['SecretBinary']


class SneakyKipper:
    task_key_aliases = {
        'pagination': 'alias/algPaginationKey'
    }

    def __init__(self, task_name):
        self._client = boto3.client('kms')
        self._key_alis = self.task_key_aliases[task_name]

    def encrypt(self, unencrypted_text, encryption_context):
        import base64
        response = self._client.encrypt(
            KeyId=self._key_alis,
            Plaintext=json.dumps(unencrypted_text),
            EncryptionContext=encryption_context
        )
        bit = base64.b64encode(response['CiphertextBlob'])
        return bit.decode()

    def decrypt(self, encrypted_text, encryption_context):
        import base64

        response = self._client.decrypt(
            CiphertextBlob=base64.b64decode(encrypted_text),
            EncryptionContext=encryption_context
        )
        return json.loads(response['Plaintext'].decode())
