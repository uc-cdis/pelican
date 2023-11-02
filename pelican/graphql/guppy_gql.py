import json
import requests

from .base_gql import BaseGQL


class GuppyGQL(BaseGQL):
    def __init__(self, node, hostname, access_token):
        super().__init__(node, hostname, access_token)

    def _count(self, filters=None):
        self.url = f"{self.hostname}/guppy/graphql"
        query = f"query($filter: JSON) {{ _aggregation {{ {self.node}(filter: $filter, accessibility: accessible) {{ _totalCount }} }} }}"
        query_json = {"query": query}
        if filters:
            query_json["variables"] = filters

        r = BaseGQL._execute(self, query_json)
        try:
            count = r["data"]["_aggregation"][self.node]["_totalCount"]
        except KeyError:
            count = 0
        return count

    def _download_endpoint(self, filters=None):
        print("fallback to /download endpoint")
        self.url = f"{self.hostname}/guppy/download"
        query = {
            "type": self.node,
            "fields": [f"_{self.node}_id"],
            "accessibility": "accessible",
        }
        if filters:
            query.update(json.loads(filters))

        r = BaseGQL._send_request(self, query)
        return r

    def _graphql_endpoint(self, filters=None):
        self.url = f"{self.hostname}/guppy/graphql"
        query = f"query($filter: JSON) {{ {self.node}(first: 10000, filter: $filter, accessibility: accessible) {{ _{self.node}_id }} }}"
        query_json = {"query": query}
        if filters:
            query_json["variables"] = filters
        r = BaseGQL._execute(self, query_json)
        try:
            r = r["data"][self.node]
        except KeyError:
            r = []
        return r

    def _graphql_auth_resource_path(self, filters=None):
        self.url = f"{self.hostname}/guppy/graphql"
        query = f"query($filter: JSON) {{ {self.node}(first: 10000, filter: $filter, accessibility: accessible) {{ auth_resource_path }} }}"
        query_json = {"query": query}
        if filters:
            query_json["variables"] = filters
        r = BaseGQL._execute(self, query_json)
        try:
            r = r["data"][self.node]
        except KeyError:
            r = []
        return r

    def execute(self, filters=None):
        count = self._count(filters)
        # Elasticsearch has a default limitation of 10000 for search results
        # https://www.elastic.co/guide/en/elasticsearch/reference/master/search-request-body.html#request-body-search-from-size
        # therefore, Pelican will use Guppy 'download' endpoint
        guppy_max_size = 10000
        r = (
            self._download_endpoint(filters)
            if count > guppy_max_size
            else self._graphql_endpoint(filters)
        )
        try:
            ids = [item[f"_{self.node}_id"] for item in r]
        except KeyError:
            ids = []
        return ids
