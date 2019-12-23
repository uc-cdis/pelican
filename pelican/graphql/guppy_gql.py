import json

from .base_gql import BaseGQL


class GuppyGQL(BaseGQL):
    def __init__(self, node, hostname, access_token):
        super().__init__(node, hostname, access_token)

    def _count(self, filters=None):
        self.url = f"{self.hostname}/guppy/graphql/"
        query = f"query ($filter: JSON) {{ _aggregation {{ {self.node}(filter: $filter, accessibility: accessible) {{ _totalCount }} }} }}"
        query_json = {"query": query}
        if filters:
            query_json["variables"] = filters

        r = BaseGQL._execute(self, query_json)
        count = r["data"]["_aggregation"][self.node]["_totalCount"]
        return count

    def execute(self, filters=None):
        if self._count(filters) > 10000:
            print("fallback to /download endpoint")
            self.url = f"{self.hostname}/guppy/download"
            query = {
                "type": self.node,
                "fields": [f"{self.node}_id"],
                "accessibility": "accessible"
            }

            query.update(json.loads(filters))

            r = BaseGQL._execute(self, query)

            return [item[f"{self.node}_id"] for item in r]

        else:
            self.url = f"{self.hostname}/guppy/graphql"
            query = f"query($filter: JSON) {{ {self.node}(first: 10000, filter: $filter, accessibility: accessible) {{ {self.node}_id }} }}"
            query_json = {"query": query}
            if filters:
                query_json["variables"] = filters

            r = BaseGQL._execute(self, query_json)
            ids = [item[f"{self.node}_id"] for item in r["data"][self.node]]
            return ids
