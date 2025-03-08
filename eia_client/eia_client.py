import datetime
import requests
from typing import Optional, Union
import polars as pl


# ================ Helper functions ================
def day_offset(start: datetime.date, end: datetime.date, offset: int) -> list:
    """
    Generate a list of date type elements with the given offset
    representing days. This helper function is for the back-fill operation,
    given that the maximum request size of the API is of approximate 2500 data points.
    """
    current = [start]
    last_date = start
    while last_date < end:
        next_date = last_date + datetime.timedelta(days=offset)
        if next_date < end:
            current.append(next_date)
            last_date = next_date
        else:
            break
    current.append(end)
    return current


def hour_offset(start: datetime.datetime, end: datetime.datetime, offset: int) -> list:
    """
    Generate a list of datetime type elements with the given offset
    representing hours. This helper function is for the back-fill operation,
    given that the maximum request size of the API is of approximate 2500 data points.
    """
    current = [start]
    last_datetime = start
    while last_datetime < end:
        next_datetime = last_datetime + datetime.timedelta(hours=offset)
        if next_datetime < end:
            current.append(next_datetime)
            last_datetime = next_datetime
        else:
            break
    current.append(end)
    return current


# ==================================================

class EIAClient:
    BASE_URL = "https://api.eia.gov/v2/"

    def __init__(self, api_key):
        self.api_key = api_key

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
        facet_str = ""

        if facets is not None:
            for i in facets.keys():
                if type(facets[i]) is list:
                    for facet in facets[i]:
                        # Un-list and concatenate facets
                        facet_str = facet_str + "&facets[" + i + "][]=" + facet
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

        # Call private method __get_data
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

    def eia_back_fill(self,
                     start,
                     end,
                     offset,
                     api_key,
                     api_path,
                     facets):

        if type(start) is datetime.date:
            time_vec_seq = self.__day_offset(start=start, end=end, offset=offset)
        elif type(start) is datetime.datetime:
            time_vec_seq = self.__hour_offset(start=start, end=end, offset=offset)

        for i in range(len(time_vec_seq[:-1])):
            start = time_vec_seq[i]
            if i < len(time_vec_seq[:-1]) - 1:
                end = time_vec_seq[i + 1] - datetime.timedelta(hours=1)
            elif i == len(time_vec_seq[:-1]) - 1:
                end = time_vec_seq[i + 1]
            temp = eia_get(api_key=api_key,
                           api_path=api_path,
                           facets=facets,
                           start=start,
                           data="value",
                           end=end)
            if i == 0:
                df = temp.data
            else:
                df = df.append(temp.data)

        parameters = {
            "api_path": api_path,
            "data": "value",
            "facets": facets,
            "start": start,
            "end": end,
            "length": None,
            "offset": offset,
            "frequency": None
        }
        output = response(data=df, parameters=parameters)
        return output
