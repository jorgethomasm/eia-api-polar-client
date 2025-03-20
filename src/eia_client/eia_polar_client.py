"""
This module contains the EIAPolarDuckClient class, which is used to interact with the EIA API.
By: Jorge Thomas https://github.com/jorgethomasm
"""
import datetime
import requests
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
import polars as pl
import duckdb


class EIAPolarClient:
    BASE_URL = "https://api.eia.gov/v2/"

    def __init__(self, api_key):
        self.api_key = api_key        

    def __get_data(self, endpoints_urls: list, params=None) -> pl.DataFrame:
        """
        Fetches data from multiple API endpoints and returns a concatenated Polars DataFrame.
        This method sends GET requests to the provided list of endpoint URLs using a thread pool
        for concurrent execution. The responses are parsed into JSON, converted into Polars
        DataFrames, and concatenated into a single DataFrame.
        Args:
            endpoints_urls (list): A list of endpoint URLs to fetch data from.
            params (dict, optional): Additional query parameters to include in the API requests.
                Defaults to None. The API key is automatically added to the parameters.
        Returns:
            pl.DataFrame: A concatenated Polars DataFrame containing the data retrieved from
            all the endpoints.
        Raises:
            requests.exceptions.RequestException: If any of the API requests fail.
            ValueError: If the resulting DataFrame is empty, indicating no data was retrieved.
        """

        params = params or {}
        params["api_key"] = self.api_key
        
        def fetch_data(url):
            """Fetch data from a single URL."""            
            response = requests.get(url=url, params=params)
            response.raise_for_status()            
            return response.json()  
     
        with ThreadPoolExecutor() as executor:
                list_with_eia_payloads = list(executor.map(fetch_data, endpoints_urls)) 

        # Generate a list of DataFrames from the list of dictionaries
        list_with_dfs = [pl.DataFrame(data["response"]["data"]) for data in list_with_eia_payloads]
   
        # Concatenate the list of DataFrames into a single DataFrame
        df = pl.concat(list_with_dfs)

        # Check if the DataFrame is empty
        if df.is_empty():
            raise ValueError("The DataFrame is empty. No data was retrieved from the API.") 
        
        return df
    
    def __get_endpoint_chunks(self, api_path, facets, start, end) -> list:
        """
        Splits a time range into chunks and generates API endpoint URLs for each chunk.
        This method divides a specified time range into smaller chunks if the range exceeds
        a predefined limit (2000 hours). It then constructs API endpoint URLs for each chunk
        based on the provided parameters.        

        Args:
            api_path (str): The API path to be appended to the base URL.
            facets (dict): A dictionary of facets to filter the API request. Each key represents
                a facet name, and the value can be a string or a list of strings.
            start (datetime): The start of the time range.
            end (datetime): The end of the time range.

        Returns:
            list: A list of strings, where each string is an API endpoint URL for a specific
            time chunk.
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
        
        # Display the number of chunks and the endpoints
        n_chunks = len(endpoints)
        if n_chunks > 1:
            print(f"\nNumber of chunks: {n_chunks}\n\nRequesting in parallel the following endpoints:\n")
            for endpoint in endpoints: 
                print(endpoint)
        else:
            print(f"\nRequesting the following endpoint:\n")
            print(endpoints[0])
        
        
        return endpoints

    def __format_df_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Format the columns types of the Polars DataFrame.
        """
        df = df.with_columns(
            [
                pl.col("value").cast(pl.Float64),
                pl.col("period") + ":00"
                ]
                )
        # Convert the period column to datetime
        df = df.with_columns(pl.col("period").str.to_datetime(format="%Y-%m-%dT%H:%M", time_zone='UTC'))  
        return df.sort("period")  


    # ================================================  
    # V2 API By Jorge Thomas
    def get_eia_data(self, 
                     api_path: str, 
                     facets: Optional[dict] = None, 
                     start: datetime.datetime = None, 
                     end: datetime.datetime = None) -> pl.DataFrame:   
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
        # ===================================

        # Generate the list of endpoints urls to be requested
        endpoints = self.__get_endpoint_chunks(api_path, facets, start, end)        

        # Get the data from the API
        df = self.__get_data(endpoints)
        
        # Format the columns and sort the DataFrame
        df = self.__format_df_columns(df) 
        
        return df
    
    
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
        