import os

from base_gql import BaseGQL


class GuppyGQL(object, BaseGQL):
    def __init__(self, hostname):
        url = "{}/{}".format(hostname, "guppy/graphql/")
        super(GuppyGQL, self).__init__()
        BaseGQL.__init__(self, url)

    def execute(self, filters=None):
        root_node = os.environ["ROOT_NODE"]

        query = (
            "query($filter: JSON) {{ {root}(first: 10000, filter: $filter) {{ {root}_id }} }}".format(root=root_node)
        )
        r = BaseGQL._execute(self, query, filters=filters)
        ids = [item["{}_id".format(root_node)] for item in r["data"][root_node]]
        return ids
