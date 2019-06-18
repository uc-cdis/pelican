from base_gql import BaseGQL


class GuppyGQL(object, BaseGQL):
    def __init__(self, hostname):
        url = "{}/{}".format(hostname, "guppy/graphql/")
        super(GuppyGQL, self).__init__()
        BaseGQL.__init__(self, url)

    def execute(self, filters=None):
        query = (
            "query($filter: JSON) { case(first: 10000, filter: $filter) { case_id } }"
        )
        r = BaseGQL._execute(self, query, filters=filters)
        ids = [case["case_id"] for case in r["data"]["case"]]
        return ids
