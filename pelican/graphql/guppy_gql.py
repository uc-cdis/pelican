from base_gql import BaseGQL


class GuppyGQL(object, BaseGQL):
    def __init__(self, node, hostname, access_token):
        super(GuppyGQL, self).__init__()
        BaseGQL.__init__(self, node, hostname, access_token)

    def _count(self, filters=None):
        self.url = "{}/{}".format(self.hostname, "guppy/graphql/")
        query = "query ($filter: JSON) {{ _aggregation {{ {node}(filter: $filter, accessibility: accessible) {{ _totalCount }} }} }}".format(
            node=self.node
        )
        query_json = {"query": query}
        if filters:
            query_json["variables"] = filters

        r = BaseGQL._execute(self, query_json)
        count = r["data"]["_aggregation"][self.node]["_totalCount"]
        return count

    def execute(self, filters=None):
        if self._count(filters) > 10000:
            print("fallback to /download endpoint")
            self.url = "{}/{}".format(self.hostname, "guppy/download")
            query = {
                "type": self.node,
                "fields": ["{}_id".format(self.node)],
                "filters": filters,
                "accessibility": "accessible"
            }
            r = BaseGQL._execute(self, query)

            return [item["{}_id".format(self.node)] for item in r]

        else:
            self.url = "{}/{}".format(self.hostname, "guppy/graphql")
            query = "query($filter: JSON) {{ {root}(first: 10000, filter: $filter, accessibility: accessible) {{ {root}_id }} }}".format(
                root=self.node
            )
            query_json = {"query": query}
            if filters:
                query_json["variables"] = filters

            r = BaseGQL._execute(self, query_json)
            ids = [item["{}_id".format(self.node)] for item in r["data"][self.node]]
            return ids
