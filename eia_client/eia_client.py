import datetime
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

    def get_electricity_data(self, api_key, api_path, data="value", facets=None, start=None, end=None, length=None,
                             offset=None, frequency=None):
        """"
        Dedicated method for route: electricity
        and hourly demand by subregion: rto
        """

        if facets is None:
            fc = ""
        else:
            fc = ""
        for i in facets.keys():
            if type(facets[i]) is list:
                for n in facets[i]:
                    fc = fc + "&facets[" + i + "][]=" + n
            elif type(facets[i]) is str:
                fc = fc + "&facets[" + i + "][]=" + facets[i]

        if start is None:
            s = ""
        else:
            if type(start) is datetime.date:
                s = "&start=" + start.strftime("%Y-%m-%d")
            elif type(start) is datetime.datetime:
                s = "&start=" + start.strftime("%Y-%m-%dT%H")
            else:
                print("Error: The start argument is not a valid date or time object")
                return

        if end is None:
            e = ""
        else:
            if type(end) is datetime.date:
                e = "&end=" + end.strftime("%Y-%m-%d")
            elif type(end) is datetime.datetime:
                e = "&end=" + end.strftime("%Y-%m-%dT%H")
            else:
                print("Error: The end argument is not a valid date or time object")
                return

        if length is None:
            l = ""
        else:
            l = "&length=" + str(length)

        if offset is None:
            o = ""
        else:
            o = "&offset=" + str(offset)

        if frequency is None:
            fr = ""
        else:
            fr = "&frequency=" + str(frequency)

        url = "https://api.eia.gov/v2/" + api_path + "?data[]=value" + fc + s + e + l + o + fr


        endpoint = f"electricity/rto/id/{series_id}"

        # Call get_data
        data = self.get_data(endpoint)  # as json

        # Convert JSON to Polars DataFrame
        df = pl.DataFrame(data["response"]["data"])

        return df
