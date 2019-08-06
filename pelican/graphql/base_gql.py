import requests


class BaseGQL:
    def __init__(self, node, url, access_token):
        self.node = node
        self.url = url
        self.access_token = access_token
        self.headers = {"Authorization": "Bearer {}".format(self.access_token)}

    def execute(self, filters=None):
        raise NotImplementedError

    def _execute(self, query):
        r = requests.post(self.url, json=query, headers=self.headers)

        if r.status_code == 200:
            return r.json()
        else:
            raise Exception("Query failed with {}.\n{}".format(r.status_code, query))
