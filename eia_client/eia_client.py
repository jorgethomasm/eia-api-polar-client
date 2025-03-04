import requests
import polars as pl


class EIAClient:
    BASE_URL = "https://api.eia.gov/v2/"

    def __init__(self, api_key):
        self.api_key = api_key

    def get_data(self, endpoint, params=None):
        params = params or {}
        params["api_key"] = self.api_key
        response = requests.get(f"{self.BASE_URL}{endpoint}", params=params)
        response.raise_for_status()
        return response.json()

    def get_electricity_data(self, series_id):
        endpoint = f"electricity/series/id/{series_id}"
        data = self.get_data(endpoint)
        # Convert JSON to Polars DataFrame
        df = pl.DataFrame(data["response"]["data"])
        return df
