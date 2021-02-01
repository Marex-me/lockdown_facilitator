import requests
import time


class APIFetcher:
    def __init__(self, request_url: str):
        self.request_url = request_url
        self.warning = None
        self.response_code = None
        self.data = None

        # Retry 3 times with 20 sec interval if response is not OK
        for _ in range(3):
            request_data = requests.get(request_url)
            self.response_code = request_data.status_code
            if self.response_code == 200:
                self.warning = None
                self.data = request_data.json()
                break
            self.warning = "Failed to fetch data after three attempts. API status: {}".format(self.response_code)
            time.sleep(15)

    def get_data(self):
        assert not self.warning, self.warning
        return self.data


class CocktailExtractor:
    def __init__(self, api_url: str, token: str, queries: dict):
        self.api_url = api_url
        self.token = token
        self.queries = queries

    def c_get_by_id(self, cid: int) -> dict:
        request_url = '/'.join([self.api_url, self.token, self.queries['id'].format(str(cid))])
        fetcher = APIFetcher(request_url)
        return fetcher.get_data()['drinks'][0]

    def c_get_by_name(self, name: str) -> dict:
        request_url = '/'.join([self.api_url, self.token, self.queries['name'].format(name)])
        fetcher = APIFetcher(request_url)
        return fetcher.get_data()['drinks'][0]

    def c_get_list_by_first_letter(self, letter: str) -> list:
        request_url = '/'.join([self.api_url, self.token, self.queries['letter'].format(letter)])
        fetcher = APIFetcher(request_url)
        return fetcher.get_data()['drinks']

    def i_get_by_id(self, iid: int) -> dict:
        request_url = '/'.join([self.api_url, self.token, self.queries['ingredient_id'].format(str(iid))])
        fetcher = APIFetcher(request_url)
        return fetcher.get_data()['ingredients'][0]

    def i_get_by_name(self, name: str) -> dict:
        request_url = '/'.join([self.api_url, self.token, self.queries['ingredient_name'].format(name)])
        fetcher = APIFetcher(request_url)
        return fetcher.get_data()['ingredients'][0]
