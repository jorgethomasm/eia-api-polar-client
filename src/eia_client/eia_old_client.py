"""
This module contains the EIA Client class, which is used to interact with the EIA API.
Original code from: Rami Krispin https://github.com/LinkedInLearning/data-pipeline-automation-with-github-actions-4503382/blob/main/python/eia_api.py
OOP Refactoring and extra methods by: Jorge Thomas https://github.com/jorgethomasm
"""
import datetime
import requests
from typing import Optional, Union
import polars as pl
import duckdb


class EIAClient:
    BASE_URL = "https://api.eia.gov/v2/"

    def __init__(self, api_key):
        self.api_key = api_key

    def __get_data(self, endpoint: str, params=None):
        params = params or {}
        params["api_key"] = self.api_key

        full_url = f"{self.BASE_URL}{endpoint}"
        print(f"Requesting...\n{full_url}")
        response = requests.get(url=full_url, params=params)
        response.raise_for_status()
        return response.json()

    def __get_data_chunk(self, endpoint: str) -> pl.DataFrame:

        # Call private method __get_data
        eia_payload = self.__get_data(endpoint)  #  dictionary
        # Convert JSON to Polars DataFrame
        df = pl.DataFrame(eia_payload["response"]["data"])  # data is a list of dicts (rows) within the response dict
        
        # Check if the DataFrame is empty
        if df.is_empty():
            raise ValueError("The DataFrame is empty. No data was retrieved from the API.")       
        
        return df

    # ================ Helper methods ================

    def __day_offset(self, start: datetime.date, end: datetime.date, offset: int) -> list:
        """
        Generate a list of date type elements with the given offset
        representing days. This helper function is for the back-fill operation,
        given that the maximum request size of the API is of approximate 2500 observations.
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

    def __hour_offset(self, start: datetime.datetime, end: datetime.datetime, offset: int) -> list:
        """
        Generate a list of datetime type elements with the given offset
        representing hours. This helper function is for the back-fill operation,
        given that the maximum request size of the API is of approximate 2500 observations.
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
    
    def __format_df_columns(self, df: pl.DataFrame, frequency: str) -> pl.DataFrame:
        """
        Format the columns types of the Polars DataFrame.
        """
        if frequency == "hourly":
            df = df.with_columns(
                [
                    pl.col("value").cast(pl.Float64),
                    pl.col("period") + ":00"
                    ]
                )
            df = df.with_columns(pl.col("period").str.to_datetime(format="%Y-%m-%dT%H:%M", time_zone='UTC'))
        elif frequency == "daily":
            df = df.with_columns(
                [
                    pl.col("value").cast(pl.Float64),
                    pl.col("period").str.to_date()                    
                    ]
                )
        else:
            raise ValueError("Frequency must be 'hourly' or 'daily'")
        
        return df    


    # ================================================  
    
    def get_eia_data(self,
                     api_path: str,
                     facets: Optional[dict] = None,
                     start: Optional[Union[datetime.date, datetime.datetime]] = None,
                     end: Optional[Union[datetime.date, datetime.datetime]] = None,
                     length: Optional[int] = None,
                     offset: Optional[int] = None,
                     frequency: Optional[str] = None) -> pl.DataFrame:        
        """ 
        Parameters
        frequency: "hourly" or "daily".
        offset: number of observations to split requests (chunks). Recommended Max. 2000.
        If offset parameters is None, the back-fill operation will not be performed!
        """
        # ===== Check input parameters =====
        
        if not isinstance(api_path, str):
            raise TypeError("api_path must be a string")        
        
        if facets is not None and not isinstance(facets, dict):
            raise TypeError("facets must be a dictionary or None")

        # Create string var for facet or extract info from the list
        facet_str = ""
        if facets is not None:
            # Extract from dictionary
            for i in facets.keys():
                if type(facets[i]) is list:
                    for facet in facets[i]:
                        # Un-list and concatenate facets strings
                        facet_str = facet_str + "&facets[" + i + "][]=" + facet
                elif type(facets[i]) is str:
                    facet_str = facet_str + "&facets[" + i + "][]=" + facets[i]

        if start is not None and not isinstance(start, (datetime.date, datetime.datetime)):
            raise TypeError("start must be a date, datetime, or None")

        if end is not None and not isinstance(end, (datetime.date, datetime.datetime)):
            raise TypeError("end must be a date, datetime, or None")

        if length is None:
            len_str = ""
        else:
            len_str = "&length=" + str(length)

        if offset is None:
            offset_str = ""
        else:
            offset_str = "&offset=" + str(offset)

        if frequency is None:
            freq_str = ""
        else:
            freq_str = "&frequency=" + frequency

        df = pl.DataFrame

        # Build url endpoint
        if offset is not None:
            # Do back-filling
            list_of_time_chunks = []

            if type(start) is datetime.date:
                list_of_time_chunks = self.__day_offset(start=start, end=end, offset=offset)

            elif type(start) is datetime.datetime:
                list_of_time_chunks = self.__hour_offset(start=start, end=end, offset=offset)

            # Moving window (chunks)
            i_chunks = len(list_of_time_chunks)-1

            for i in range(0, i_chunks):

                start = list_of_time_chunks[i]
                if i < i_chunks-1:
                    end = list_of_time_chunks[i+1] - datetime.timedelta(hours=1)
                elif i == i_chunks-1:
                    end = list_of_time_chunks[i+1]

                # Start and End chunks
                if start is None:
                    start_str = ""
                elif type(start) is datetime.date:
                    start_str = "&start=" + start.strftime("%Y-%m-%d")
                else:
                    start_str = "&start=" + start.strftime("%Y-%m-%dT%H")

                if end is None:
                    end_str = ""
                elif type(end) is datetime.date:
                    end_str = "&end=" + end.strftime("%Y-%m-%d")
                else:
                    end_str = "&end=" + end.strftime("%Y-%m-%dT%H")

                # Write endpoint urls
                endpoint = (api_path + "?data[]=value" + facet_str + start_str + end_str + len_str + freq_str)

                df_temp = self.__get_data_chunk(endpoint)

                if i == 0:
                    df = df_temp  # First fill
                else:
                    # Back-fill the rest                                        
                    df.extend(df_temp)  # append in place
        
        else:
            
            # Do not do back-filling
            if start is None:
                start_str = ""
            elif type(start) is datetime.date:
                start_str = "&start=" + start.strftime("%Y-%m-%d")
            else:
                start_str = "&start=" + start.strftime("%Y-%m-%dT%H")

            if end is None:
                end_str = ""
            elif type(end) is datetime.date:
                end_str = "&end=" + end.strftime("%Y-%m-%d")
            else:
                end_str = "&end=" + end.strftime("%Y-%m-%dT%H")

            # Write endpoint url
            endpoint = (api_path + "?data[]=value" + facet_str + start_str + end_str + len_str + offset_str + freq_str)
            df = self.__get_data_chunk(endpoint)  

        # Format the columns of the DataFrame
        df = self.__format_df_columns(df, frequency=frequency) 

        # Sort the DataFrame by period
        return df.sort("period")
    
    
    def save_df_as_duckdb(self, df: pl.DataFrame, path: str = "./data/raw/eia_data.duckdb", table_name: str = "eia_data") -> None:
        """
        Save a Polars DataFrame with the requested EIA data to a DuckDB file.
        Ideal for large dynamic (updatable) dataset and quick data analysis.
        """
        query = f"CREATE TABLE {table_name} AS SELECT * FROM df"
        con = duckdb.connect(path)
        con.execute(query)
        con.close()
                
        return None  
        