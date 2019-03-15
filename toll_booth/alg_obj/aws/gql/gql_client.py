import json

import requests

from toll_booth.alg_obj.aws.gql.gql_notary import GqlNotary
from toll_booth.alg_obj.aws.moorland import cos


class GqlConnection:
    def __init__(self, gql_url=None, session=None):
        if not gql_url:
            gql_url = cos.getenv(
                'GQL_URL', 'https://vi2wfvboq5aozlqmstb24p5tbq.appsync-api.us-east-1.amazonaws.com/graphql')
        gql_base = gql_url.replace('/graphql', '')
        gql_base = gql_base.replace('https://', '')
        if not session:
            session = requests.session()
        self._gql_url = gql_url
        self._notary = GqlNotary(gql_base)
        self._session = session

    def query(self, query_text, variables):
        headers = self._notary.generate_headers(query_text, variables)
        payload = {'query': query_text, 'variables': variables}
        request = requests.post(self._gql_url, headers=headers, json=payload)
        if request.status_code != 200:
            raise RuntimeError(request.content)
        return request.text


class GqlClient:
    def __init__(self, gql_url=None):
        self._connection = GqlConnection(gql_url)

    def query(self, query_text, variables):
        results = self._connection.query(query_text, variables)
        return json.loads(results)

