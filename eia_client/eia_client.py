import datetime
import requests
from typing import Optional, Union
import polars as pl


class EIAClient:
    BASE_URL = "https://api.eia.gov/v2/"

    def __init__(self, api_key):
        self.api_key = api_key

    # Helper (private) methods:
    def __day_offset(start, end, offset) -> list:
        current = [start]
        while max(current) < end:
            if max(current) + datetime.timedelta(days=offset) < end:
                current.append(max(current) + datetime.timedelta(days=offset))
        else:
            current.append(end)
        return current

    def __hour_offset(start, end, offset) -> list:
        current = [start]
        while max(current) < end:
            if max(current) + datetime.timedelta(hours=offset) < end:
                current.append(max(current) + datetime.timedelta(hours=offset))
            else:
                current.append(end)
        return current

    def __get_data(self, endpoint: str, params=None):
        params = params or {}
        params["api_key"] = self.api_key

        full_url = f"{self.BASE_URL}{endpoint}"
        response = requests.get(url=full_url, params=params)
        response.raise_for_status()
        return response.json()

    def get_electricity_data(self, 
                             api_path: str, 
                             facets: Optional[dict] = None,
                             start: Optional[Union[datetime.date, datetime.datetime]] = None,
                             end: Optional[Union[datetime.date, datetime.datetime]] = None,
                             length: Optional[str] = None,
                             offset: Optional[str] = None,
                             frequency: Optional[str] = None) -> pl.DataFrame:
        """"
        route: electricity
        rto: real-time grid monitor
        """
        
        # Check if facets is not a string, list, or None
        if facets is not None and not isinstance(facets, dict):
            raise TypeError("facets must be a dictionary or None")

        # Create string var for facet or extract info from the list
        if facets is None:
            facet_str = ""
        else:
            facet_str = ""
            for i in facets.keys():
                if type(facets[i]) is list:
                    for n in facets[i]:
                        facet_str = facet_str + "&facets[" + i + "][]=" + n
                elif type(facets[i]) is str:
                    facet_str = facet_str + "&facets[" + i + "][]=" + facets[i]

        if start is not None and not isinstance(start, (datetime.date, datetime.datetime)):
            raise TypeError("start must be a date, datetime, or None")

        if start is None:
            start = ""
        elif type(start) is datetime.date:
            start = "&start=" + start.strftime("%Y-%m-%d")
        else:
            start = "&start=" + start.strftime("%Y-%m-%dT%H")           

        if end is not None and not isinstance(end, (datetime.date, datetime.datetime)):
            raise TypeError("end must be a date, datetime, or None")

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

        endpoint = "electricity/" + api_path + "?data[]=value" + facet_str + start + end + length + offset + frequency

        # Call get_data
        data = self.__get_data(endpoint)  # as json

        # Convert JSON to Polars DataFrame
        df = pl.DataFrame(data["response"]["data"])

        # Reformating the output
        df = df.with_columns(
            [
                pl.col("value").cast(pl.Float64),
                pl.col("period") + ":00"
            ]
        )
        df = df.with_columns(pl.col("period").str.to_datetime(format="%Y-%m-%dT%H:%M", time_zone='UTC'))

        return df

    # def eia_backfill(start, end, offset, api_key, api_path, facets):
    #         class response:
    #             def __init__(output, data, parameters):
    #                 output.data = data
    #                 output.parameters = parameters
    #
    #         if type(api_key) is not str:
    #             print("Error: The api_key argument is not a valid string")
    #             return
    #         elif len(api_key) != 40:
    #             print("Error: The length of the api_key is not valid, must be 40 characters")
    #             return
    #
    #         if api_path[-1] != "/":
    #             api_path = api_path + "/"
    #
    #         if type(start) is datetime.date:
    #             s = "&start=" + start.strftime("%Y-%m-%d")
    #         elif type(start) is datetime.datetime:
    #             s = "&start=" + start.strftime("%Y-%m-%dT%H")
    #         else:
    #             print("Error: The start argument is not a valid date or time object")
    #             return
    #
    #         if type(end) is datetime.date:
    #             e = "&end=" + end.strftime("%Y-%m-%d")
    #         elif type(end) is datetime.datetime:
    #             e = "&end=" + end.strftime("%Y-%m-%dT%H")
    #         else:
    #             print("Error: The end argument is not a valid date or time object")
    #             return
    #
    #         if type(start) is datetime.date:
    #             time_vec_seq = day_offset(start=start, end=end, offset=offset)
    #         elif type(start) is datetime.datetime:
    #             time_vec_seq = hour_offset(start=start, end=end, offset=offset)
    #
    #         for i in range(len(time_vec_seq[:-1])):
    #             start = time_vec_seq[i]
    #             if i < len(time_vec_seq[:-1]) - 1:
    #                 end = time_vec_seq[i + 1] - datetime.timedelta(hours=1)
    #             elif i == len(time_vec_seq[:-1]) - 1:
    #                 end = time_vec_seq[i + 1]
    #             temp = eia_get(api_key=api_key,
    #                            api_path=api_path,
    #                            facets=facets,
    #                            start=start,
    #                            data="value",
    #                            end=end)
    #             if i == 0:
    #                 df = temp.data
    #             else:
    #                 df = df._append(temp.data)
    #
    #         parameters = {
    #             "api_path": api_path,
    #             "data": "value",
    #             "facets": facets,
    #             "start": start,
    #             "end": end,
    #             "length": None,
    #             "offset": offset,
    #             "frequency": None
    #         }
    #         output = response(data=df, parameters=parameters)
    #         return output
