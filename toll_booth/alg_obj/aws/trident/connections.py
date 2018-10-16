import datetime
import hashlib
import hmac
import json
import os
import urllib.parse

import requests

from toll_booth.alg_obj.aws.aws_obj.squirrel import Opossum
from toll_booth.alg_obj.aws.trident.trident_obj import TridentVertex, TridentEdge, TridentProperty, TridentPath


class TridentDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    @staticmethod
    def object_hook(obj):
        if '@type' not in obj:
            return obj
        obj_type = obj['@type']
        obj_value = obj['@value']
        if obj_type == 'g:T':
            if obj_value == 'id':
                return 'internal_id'
            if obj_value == 'label':
                return 'label'
        if obj_type == 'g:Int32':
            return int(obj_value)
        if obj_type == 'g:Int64':
            return int(obj_value)
        if obj_type == 'g:List':
            return obj_value
        if obj_type == 'g:Set':
            return set(obj_value)
        if obj_type == 'g:Map':
            created_map = {}
            i = 0
            while i < len(obj_value):
                created_map[obj_value[i]] = obj_value[i + 1]
                i += 2
            return created_map
        if obj_type == 'g:Vertex':
            try:
                trident_vertex = TridentVertex(obj_value['id'], obj_value['label'], obj_value['properties'])
                return trident_vertex
            except KeyError:
                return TridentVertex(obj_value['id'], obj_value['label'])
        if obj_type == 'g:Edge':
            return TridentEdge(obj_value['id'], obj_value['label'],
                               obj_value['inV'], obj_value['inVLabel'],
                               obj_value['outV'], obj_value['outVLabel'])
        if obj_type == 'g:VertexProperty':
            return TridentProperty(obj_value['label'], obj_value['value'])
        if obj_type == 'g:Path':
            return TridentPath(obj_value['labels'], obj_value['objects'])
        return obj


class TridentNotary:
    _region = os.getenv('AWS_REGION', 'us-east-1')
    _service = 'neptune-db'

    def __init__(self, neptune_endpoint, session=None):
        if not session:
            session = requests.session()
        self._session = session
        self._neptune_endpoint = neptune_endpoint
        self._uri = '/gremlin/'
        self._method = 'GET'
        self._host = neptune_endpoint + ':8182'
        self._signed_headers = 'host;x-amz-date'
        self._algorithm = 'AWS4-HMAC-SHA256'
        access_key, secret_key = Opossum.get_trident_user_key()
        self._access_key = access_key
        self._secret_key = secret_key
        self._credentials = f"Credentials={self._access_key}"
        self._request_url = f'http://{neptune_endpoint}:8182{self._uri}'

    @classmethod
    def get_for_writer(cls, **kwargs):
        endpoint = kwargs.get('neptune_endpoint', os.getenv('NEPTUNE_ENDPOINT', ''))
        return cls(endpoint)

    @classmethod
    def get_for_reader(cls, **kwargs):
        endpoint = kwargs.get('graph_reader_endpoint', os.getenv('GRAPH_READER_ENDPOINT', ''))
        return cls(endpoint)

    def send(self, command):
        t = datetime.datetime.utcnow()
        amz_date = t.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = t.strftime('%Y%m%d')
        canonical_request, request_parameters = self._generate_canonical_request(amz_date, command)
        credential_scope = self._generate_scope(date_stamp)
        string_to_sign = self._generate_string_to_sign(canonical_request, amz_date, credential_scope)
        signature = self._generate_signature(string_to_sign, date_stamp)
        headers = self._generate_headers(credential_scope, signature, amz_date)
        get_results = self._session.get(self._request_url, headers=headers, verify=False, params=request_parameters)
        if get_results.status_code != 200:
            raise RuntimeError(f'error passing command to remote database: {get_results.text}, command: {command}')
        response_json = json.loads(get_results.text)
        results = response_json['result']['data']
        results = json.loads(json.dumps(results), cls=TridentDecoder)
        return results

    def _generate_canonical_request(self, amz_date, command):
        payload = {'gremlin': command}
        request_parameters = urllib.parse.urlencode(payload, quote_via=urllib.parse.quote)
        canonical_headers = f'host:{self._host}\nx-amz-date:{amz_date}\n'
        payload_hash = hashlib.sha256(''.encode('utf-8')).hexdigest()
        canonical_request = f"{self._method}\n{self._uri}\n{request_parameters}\n{canonical_headers}\n{self._signed_headers}\n{payload_hash}"
        return canonical_request, request_parameters

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
        return {'x-amz-date': amz_date, 'Authorization': authorization_header}

    @classmethod
    def _generate_request_parameters(cls, command):
        payload = {'gremlin': command}
        request_parameters = urllib.parse.urlencode(payload, quote_via=urllib.parse.quote)
        payload_hash = hashlib.sha256(''.encode('utf-8')).hexdigest()
        return payload_hash, request_parameters

    @classmethod
    def _sign(cls, key, message):
        return hmac.new(key, message.encode('utf-8'), hashlib.sha256).digest()
