import requests
import json


class BaseGQL:
    def __init__(self, node, hostname, access_token):
        self.node = node
        self.hostname = hostname
        self.url = None
        self.access_token = access_token
        self.headers = {"Authorization": f"Bearer {self.access_token}",  "Content-Type": "application/json"}

    def execute(self, filters=None):
        raise NotImplementedError

    def _execute(self, query):
        # Ensure that the variables are encoded for the POST request
        var = query["variables"]
        var = json.loads(var)
        query["variables"] = var

        r = requests.post(self.url, json=query, headers=self.headers)
        
        if r.status_code == 200:
            return r.json()
        else:
            raise Exception(f"Query failed with {r.status_code}.\n{query}")
