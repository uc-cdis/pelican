import json

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

    def _mapping(self):
        self.url = f"{self.hostname}/guppy/graphql"
        query = f"query {{ _mapping {{ {self.node} }} }}"
        query_json = {"query": query}

        r = BaseGQL._execute(self, query_json)
        try:
            mapping_list = r["data"]["_mapping"][self.node]
            node_name = [s for s in mapping_list if f"{self.node}_id" in s][0]
        except IndexError:
            node_name = None
        return node_name

    def _download_endpoint(self, filters=None, node_name=None):
        print("fallback to /download endpoint")
        if node_name is None:
            node_name = f"_{self.node}_id"
        self.url = f"{self.hostname}/guppy/download"
        query = {
            "type": self.node,
            "fields": [node_name],
            "accessibility": "accessible",
        }
        if filters:
            query.update(json.loads(filters))
        r = BaseGQL._execute(self, query)
        return r

    def _graphql_endpoint(self, filters=None, node_name=None):
        if node_name is None:
            node_name = f"_{self.node}_id"
        self.url = f"{self.hostname}/guppy/graphql"
        query = f"query($filter: JSON) {{ {self.node}(first: 10000, filter: $filter, accessibility: accessible) {{ {node_name} }} }}"
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
        node_name = self._mapping()
        # Elasticsearch has a default limitation of 10000 for search results
        # https://www.elastic.co/guide/en/elasticsearch/reference/master/search-request-body.html#request-body-search-from-size
        # therefore, Pelican will use Guppy 'download' endpoint
        guppy_max_size = 10000
        r = (
            self._download_endpoint(filters, node_name)
            if count > guppy_max_size
            else self._graphql_endpoint(filters, node_name)
        )
        try:
            ids = [item[f"{node_name}"] for item in r]
        except KeyError:
            ids = []
        return ids
