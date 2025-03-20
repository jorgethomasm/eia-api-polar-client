"""
This module contains the EIAPolarDuckClient class, which is used to interact with the EIA API.

By: Jorge Thomas https://github.com/jorgethomasm
"""
import datetime
import requests
from typing import Optional, Union
import polars as pl
import duckdb


class EIAPolarDuckClient:
    BASE_URL = "https://api.eia.gov/v2/"

    def __init__(self, api_key):
        self.api_key = api_key    

    def __get_data_chunks(self, endpoint: str, params=None) -> pl.DataFrame:

        params = params or {}
        params["api_key"] = self.api_key

        # TODO: Make concurrent requests to the API
        full_url = f"{self.BASE_URL}{endpoint}"
        print(f"Requesting...\n{full_url}")
        response = requests.get(url=full_url, params=params)
        response.raise_for_status()
        
        eia_payload = response.json()  #  dictionary
        
        # Convert JSON to Polars DataFrame
        df = pl.DataFrame(eia_payload["response"]["data"])  # data is a list of dicts (rows) within the response dict
        
        # Check if the DataFrame is empty
        if df.is_empty():
            raise ValueError("The DataFrame is empty. No data was retrieved from the API.")       
        
        return df
    
    def __get_endpoint_chunks(self, api_path, facets, start, end) -> list:
        """
        Get the data chunks from the API and return a list of Polars DataFrames.
        """
        offset = 2000
        frequency = "hourly"  # always hourly  
        len_str = "" 
        freq_str = "&frequency=" + frequency
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

            # Initialize DataFrame
        df = pl.DataFrame().with_columns(
            period=pl.datetime_range(start=start, end=end, interval="1h", closed="both", eager=True)
            )
        
        dt_starts, dt_ends = [], []
        if df.height > 2000:
            # Split requests in chunks in heights of 2000            
            chunks = [df.slice(i, offset) for i in range(0, len(df), offset)]
            for chunk in chunks:
                dt_starts.append(chunk["period"][0])
                dt_ends.append(chunk["period"][-1])     
        else:
            dt_starts.append(df["period"][0])
            dt_ends.append(df["period"][-1])

        # Build list of endpoints for each chunk   
        endpoints = []
        for i in range(len(dt_starts)):
            start_str = "&start=" + dt_starts[i].strftime("%Y-%m-%dT%H")  # Format: # 2024-01-01T01
            end_str = "&end=" + dt_ends[i].strftime("%Y-%m-%dT%H")   
            endpoints.append(self.BASE_URL + api_path + "?data[]=value" + facet_str + start_str + end_str + len_str + freq_str) 
        
        return endpoints

    # ================ Helper methods ================
    
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
    # V2 API
    def get_eia_data(self,
                     api_path: str,
                     facets: Optional[dict] = None,
                     start: datetime.datetime = None,
                     end: datetime.datetime = None                     
                     ) -> pl.DataFrame:       
                
        """ 
        Parameters
        frequency: always "hourly".
        offset: number of observations to split requests (chunks). Recommended Max. 2000       
        """            

        # ===== Check input parameters =====
        
        if not isinstance(api_path, str):
            raise TypeError("api_path must be a string")        
        
        if facets is not None and not isinstance(facets, dict):
            raise TypeError("facets must be a dictionary or None")        

        if not isinstance(start, datetime.datetime):
            raise TypeError("start must be a datetime")

        if not isinstance(end, datetime.datetime):
            raise TypeError("end must be a datetime")    
        
        endpoints = self.__get_endpoint_chunks(api_path, facets, start, end)


        
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
        