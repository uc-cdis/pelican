import requests


class BaseGQL:
    def __init__(self, node, hostname, access_token):
        self.node = node
        self.hostname = hostname
        self.url = None
        self.access_token = access_token
        self.headers = {"Authorization": f"Bearer {self.access_token}"}

    def execute(self, filters=None):
        raise NotImplementedError

    def _execute(self, query):
        r = requests.post(self.url, json=query, headers=self.headers)

        if r.status_code == 200:
            return r.json()
        else:
            raise Exception(f"Query failed with {r.status_code}.\n{query}")
