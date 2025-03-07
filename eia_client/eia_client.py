import datetime
import requests
from typing import Optional, Union
import polars as pl


class EIAClient:
    BASE_URL = "https://api.eia.gov/v2/"

    def __init__(self, api_key):
        self.api_key = api_key

    def get_data(self, endpoint: str, params=None):
        params = params or {}
        params["api_key"] = self.api_key

        response = requests.get(f"{self.BASE_URL}{endpoint}", params=params)
        response.raise_for_status()
        return response.json()

    def get_electricity_data(self, 
                             api_path: str, 
                             facets: Optional[dict], 
                             start: Optional[Union[datetime.date, datetime.datetime]], 
                             end=Optional[Union[datetime.date, datetime.datetime]], 
                             length=Optional[str],
                             offset=Optional[str], 
                             frequency=Optional[str]) -> pl.DataFrame:
        """"
        route: electricity
        rto: real-time grid monitor
        """
        
        # Check if facets is not a string, list, or None
        if facets is not None and not isinstance(facets, dict):
            raise TypeError("facets must be a dictionary or None")

        if facets is None:
            facets = ""
        else:
            for i in facets.keys():
                if type(facets[i]) is list:
                    for n in facets[i]:
                        facets = facets + "&facets[" + i + "][]=" + n
                elif type(facets[i]) is str:
                    facets = facets + "&facets[" + i + "][]=" + facets[i]

        if start is not None and not isinstance(start,(datetime.date, datetime.datetime)):
            raise TypeError("start must be a date, datetime, or None")

        if start is None:
            start = ""
        elif type(start) is datetime.date:
            start = "&start=" + start.strftime("%Y-%m-%d")
        else:
            start = "&start=" + start.strftime("%Y-%m-%dT%H")           

        if end is not None and not isinstance(end,(datetime.date, datetime.datetime)):
            raise TypeError("start must be a date, datetime, or None")

        if end is None:
            end = ""
        elif type(end) is datetime.date:
            end = "&start=" + end.strftime("%Y-%m-%d")
        else:
            end = "&start=" + end.strftime("%Y-%m-%dT%H")

        if length is None:
            length = ""
        else:
            length = "&length=" + str(length)

        if offset is None:
            offset = ""
        else:
            offset = "&offset=" + str(offset)

        if frequency is None:
            frequency = ""
        else:
            frequency = "&frequency=" + str(frequency)

        endpoint = "electricity/" + api_path + "?data[]=value" + facets + start + end + length + offset + frequency        

        # Call get_data
        data = self.get_data(endpoint)  # as json

        # Convert JSON to Polars DataFrame
        df = pl.DataFrame(data["response"]["data"])

        return df
