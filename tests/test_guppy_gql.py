from unittest import TestCase

import json
import responses
from functools import partial

from pelican.graphql.guppy_gql import GuppyGQL


class TestGraphQLEndpoint(TestCase):
    TEST_TOKEN = "valid1"
    INVALID_TOKEN = "1nval1d"
    @classmethod
    def setUpClass(cls):
        cls.node = "case"

    def setUp(self):
        def request_callback(request):
            p = 100
            cases = [{f"{self.node}_id": str(n)} for n in range(p)]

            auth_token = request.headers.get('Authorization', '')[7:]
            if not auth_token == self.TEST_TOKEN:
                return 401, {}, '{"detail": "Bad token"}'

            endpoint = request.url.strip('/').split('/')[-1]
            payload = json.loads(request.body)

            body = {}
            if endpoint == 'graphql':
                if "_aggregation" in payload["query"]:
                    body = {"data": {"_aggregation": {self.node: {"_totalCount": len(cases)}}}}
                else:
                    body = {"data": {node: cases}}
            elif endpoint == 'download':
                body = cases

            headers = {}
            return 200, headers, json.dumps(body)

        responses.add_callback(
            responses.POST, "https://localhost/guppy/graphql",
            callback=request_callback,
            content_type="application/json",
        )
        responses.add_callback(
            responses.POST, "https://localhost/guppy/download",
            callback=request_callback,
            content_type="application/json",
        )
        # self.gql = GuppyGQL(node=self.node, hostname="https://localhost", access_token="any")

    @responses.activate
    def test_execute_valid_token(self):
        gql = GuppyGQL(node=self.node, hostname="https://localhost", access_token=self.TEST_TOKEN)
        self.assertEqual(0, len(gql.execute()))

    @responses.activate
    def test_execute_invalid_token(self):
        gql = GuppyGQL(node=self.node, hostname="https://localhost", access_token=self.INVALID_TOKEN)
        self.assertEqual(0, len(gql.execute()))

    # @responses.activate
    # def test_execute(self):
    #     for p in [100, 20000]:
    #         with self.subTest(p=p):
    #             cases = [{f"{self.node}_id": str(n)} for n in range(p)]
    #
    #             def request_callback(request, node=None, cases=None):
    #                 auth_token = request.headers.get('Authorization', '')[7:]
    #                 if not auth_token == 'any':
    #                     return 401, {}, '{"detail": "Bad token"}'
    #
    #                 endpoint = request.url.strip('/').split('/')[-1]
    #                 payload = json.loads(request.body)
    #
    #                 if endpoint == 'graphql':
    #                     if "_aggregation" in payload["query"]:
    #                         body = {"data": {"_aggregation": {self.node: {"_totalCount": len(cases)}}}}
    #                     else:
    #                         body = {"data": {node: cases}}
    #                 elif endpoint == 'download':
    #                     body = cases
    #
    #                 headers = {}
    #                 return 200, headers, json.dumps(body)
    #
    #             responses.reset()
    #
    #             responses.add_callback(
    #                 responses.POST, "https://localhost/guppy/graphql",
    #                 callback=partial(request_callback, node=self.node, cases=cases),
    #                 content_type="application/json",
    #             )
    #             responses.add_callback(
    #                 responses.POST, "https://localhost/guppy/download",
    #                 callback=partial(request_callback, node=self.node, cases=cases),
    #                 content_type="application/json",
    #             )
    #
    #             self.assertEqual(p, len(self.gql.execute()))
