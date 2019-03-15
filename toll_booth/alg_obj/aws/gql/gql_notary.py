import datetime
import hashlib
import hmac
import json
import os
import urllib.parse

from toll_booth.alg_obj.aws.squirrels.squirrel import Opossum


class GqlNotary:
    _region = os.getenv('AWS_REGION', 'us-east-1')
    _service = 'appsync'

    def __init__(self, gql_endpoint_base):
        self._host = gql_endpoint_base
        self._endpoint = f'https://{gql_endpoint_base}/'
        self._uri = '/graphql'
        self._method = 'POST'
        self._signed_headers = 'host;x-amz-date'
        self._algorithm = 'AWS4-HMAC-SHA256'
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self._session_token = os.getenv('AWS_SESSION_TOKEN', None)
        if access_key is None or secret_key is None:
            access_key, secret_key = Opossum.get_trident_user_key()
        self._access_key = access_key
        self._secret_key = secret_key
        self._credentials = f"Credentials={self._access_key}"

    def generate_headers(self, query, variables):
        t = datetime.datetime.utcnow()
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = t.strftime('%Y%m%d')
        canonical_request = self._generate_canonical_request(amz_date, query, variables)
        credential_scope = self._generate_scope(date_stamp)
        string_to_sign = self._generate_string_to_sign(canonical_request, amz_date, credential_scope)
        signature = self._generate_signature(string_to_sign, date_stamp)
        headers = self._generate_headers(credential_scope, signature, amz_date)
        return headers

    def _generate_canonical_request(self, amz_date, query, variables):
        payload = {'query': query, 'variables': variables}
        canonical_headers = f'host:{self._host}\nx-amz-date:{amz_date}\n'
        payload_hash = hashlib.sha256(json.dumps(payload).encode('utf-8')).hexdigest()
        canonical_request = f"{self._method}\n{self._uri}\n\n{canonical_headers}\n{self._signed_headers}\n{payload_hash}"
        return canonical_request

    def _generate_string_to_sign(self, canonical_request, amz_date, scope):
        hash_request = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
        return f"{self._algorithm}\n{amz_date}\n{scope}\n{hash_request}"

    def _generate_scope(self, date_stamp):
        return f"{date_stamp}/{self._region}/{self._service}/aws4_request"

    def _get_signature_key(self, date_stamp):
        k_date = self._sign(f'AWS4{self._secret_key}'.encode('utf-8'), date_stamp)
        k_region = self._sign(k_date, self._region)
        k_service = self._sign(k_region, self._service)
        k_signing = self._sign(k_service, 'aws4_request')
        return k_signing

    def _generate_signature(self, string_to_sign, date_stamp):
        signing_key = self._get_signature_key(date_stamp)
        signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
        return signature

    def _generate_headers(self, credential_scope, signature, amz_date):
        credentials_entry = f'Credential={self._access_key}/{credential_scope}'
        headers_entry = f'SignedHeaders={self._signed_headers}'
        signature_entry = f'Signature={signature}'
        authorization_header = f"{self._algorithm} {credentials_entry}, {headers_entry}, {signature_entry}"
        headers = {'x-amz-date': amz_date, 'Authorization': authorization_header, 'Content-Type': "application/graphql"}
        if self._session_token:
            headers['x-amz-security-token'] = self._session_token
        return headers

    @classmethod
    def _generate_request_parameters(cls, command):
        payload = {'gremlin': command}
        request_parameters = urllib.parse.urlencode(payload, quote_via=urllib.parse.quote)
        payload_hash = hashlib.sha256(''.encode('utf-8')).hexdigest()
        return payload_hash, request_parameters

    @classmethod
    def _sign(cls, key, message):
        return hmac.new(key, message.encode('utf-8'), hashlib.sha256).digest()
