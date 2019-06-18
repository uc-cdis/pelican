import os
import requests


class BaseGQL:
    def __init__(self, url):
        self.url = url

    def get_token(self):
        access_token = os.environ["ACCESS_TOKEN"]
        headers = {"Authorization": "Bearer {}".format(access_token)}
        return headers

    def execute(self, filters=None):
        raise NotImplementedError

    def _execute(self, query, filters=None, cookies=None):
        """
        Runs GraphQL script with cookies to get the list of all ids

        :param query: the actual GraphQL query to run
        :param cookies: (optional) list of cookies, must have "access_token" there
        :return: resulting data
        """
        query_json = {"query": query}
        if filters:
            query_json["variables"] = filters
        r = requests.post(
            self.url, json=query_json, headers=self.get_token(), cookies=cookies
        )

        if r.status_code == 200:
            return r.json()
        else:
            raise Exception("Query failed with {}.\n{}".format(r.status_code, query))
