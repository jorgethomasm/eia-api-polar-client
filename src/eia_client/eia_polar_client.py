"""
This module contains the EIAPolarClient class, which is used to interact with the EIA API.
By: Jorge Thomas https://github.com/jorgethomasm
Date: 2025-03-01
"""

import datetime
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from typing import Optional

import duckdb
import polars as pl
import requests


class EIAPolarClient:
    """
    A client to interact with the U.S. Energy Information Administration (EIA) API using Polars DataFrames.
    The client provides methods to fetch data from the EIA API, format the data into Polars DataFrames, and save
    the data to a DuckDB file. Ideal for hourly time series."""

    BASE_URL = "https://api.eia.gov/v2/"

    def __init__(self, api_key):
        self.api_key = api_key

    def __fetch_data(self, url: str, params: dict) -> dict:
        """
        Fetch data from a single URL.
        Args:
            url (str): The API endpoint URL.
            params (dict): Query parameters for the API request.
        Returns:
            dict: The JSON response from the API.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        response = requests.get(url=url, params=params)
        response.raise_for_status()
        return response.json()

    def __probe_data(self, endpoint_url: str, params=None) -> int:
        """Fetch one hour of data to check how the chunks will be divided.
        Args:
            endpoint_url (str): The API endpoint URL.
            params (dict): Query parameters for the API request.
        Returns:
            int: number of time series available, i.e. divisor for chunk_size."""
        params = params or {}
        params["api_key"] = self.api_key
        probe_payload = self.__fetch_data(url=endpoint_url, params=params)

        df_probe = pl.DataFrame(probe_payload["response"]["data"])

        # Check if the DataFrame is empty
        if df_probe.is_empty():
            raise ValueError(
                "The DataFrame is empty. No data was retrieved from the API."
            )

        n_timeseries = df_probe.height
        print(f"\nNumber of time series requested: {n_timeseries}")

        return n_timeseries

    def __get_data_as_df(self, endpoints_urls: list, params=None) -> pl.DataFrame:
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

        # Ad-hoc function to map with ThreadPoolExecutor
        # def fetch_data(url:str) -> dict:
        #     """Fetch data from a single URL."""
        #     response = requests.get(url=url, params=params)
        #     response.raise_for_status()
        #     return response.json()

        with ThreadPoolExecutor() as executor:
            list_with_eia_payloads = list(
                executor.map(lambda url: self.__fetch_data(url, params), endpoints_urls)
            )
            # list_with_eia_payloads = list(executor.map(self.__fetch_data, endpoints_urls))

        # Generate a list of DataFrames from the list of dictionaries
        list_with_dfs = [
            pl.DataFrame(data["response"]["data"]) for data in list_with_eia_payloads
        ]

        # Concatenate the list of DataFrames into a single DataFrame
        df = pl.concat(list_with_dfs)

        # Check if the DataFrame is empty
        if df.is_empty():
            raise ValueError(
                "The DataFrame is empty. No data was retrieved from the API."
            )

        return df

    def __generate_probe_endpoint(self, api_path, facets, start, end) -> list:
        """
        Generates a probe endpoint URL for the API based on the provided parameters.
        Args:
            api_path (str): The API path to be appended to the base URL.
            facets (dict): A dictionary of facets where keys are facet names and values are either
                           strings or lists of strings representing facet values.
            start (datetime.datetime): The start datetime for the data query.
            end (datetime.datetime): The end datetime for the data query (not used in the current implementation).
        Returns:
            str: The generated probe endpoint URL as a string.
        Notes:
            - The frequency is hardcoded to "hourly".
            - The `end` parameter is not directly used in the function, but the probe endpoint
              uses `start` and adds one hour to it to define the end time for the probe.
        """
        frequency = "hourly"  # always hourly
        len_str = ""
        freq_str = "&frequency=" + frequency
        # Create string var for facet or extract info from the list
        facet_str = self.__concat_facets_string(facets=facets)

        # Build probe endpoint, i.e. probe url
        df_end_probe = start + datetime.timedelta(hours=1)
        probe_endpoint = (
            self.BASE_URL
            + api_path
            + "?data[]=value"
            + facet_str
            + "&start="
            + start.strftime("%Y-%m-%dT%H")
            + "&end="
            + df_end_probe.strftime("%Y-%m-%dT%H")
            + len_str
            + freq_str
        )

        return probe_endpoint

    def __generate_endpoint_chunks(
        self, api_path, facets, start, end, max_rows_request, n_timeseries
    ) -> list:
        """
        Splits a time range into chunks and generates API endpoint URLs for each chunk.
        This method divides a specified time range into smaller chunks if the range exceeds
        a predefined limit max_row_request (~4000 hours = rows). It then constructs API endpoint URLs for each chunk
        based on the provided parameters.

        Args:
            api_path (str): The API path to be appended to the base URL.
            facets (dict): A dictionary of facets to filter the API request. Each key represents
                a facet name, and the value can be a string or a list of strings.
            start (datetime): The start of the time range.
            end (datetime): The end of the time range.

        Returns:
            list: A list of strings, where each string is an API endpoint URL for a specific
            time chunk. The first element is the probe endpoint URL.
        """
        chunk_size = ceil(max_rows_request / n_timeseries)

        if chunk_size % 2 != 0:  # Check if it's odd
            chunk_size += 1

        frequency = "hourly"  # always hourly
        len_str = ""
        freq_str = "&frequency=" + frequency
        # Create string var for facet or extract info from the list
        facet_str = self.__concat_facets_string(facets=facets)

        # Initialize DataFrame
        df = pl.DataFrame().with_columns(
            period=pl.datetime_range(
                start=start, end=end, interval="1h", closed="both", eager=True
            )
        )

        dt_starts, dt_ends = [], []
        if df.height > chunk_size:
            # Split requests in chunks in heights of max_rows_request
            chunks = [df.slice(i, chunk_size) for i in range(0, len(df), chunk_size)]
            for chunk in chunks:
                dt_starts.append(chunk["period"][0])
                dt_ends.append(chunk["period"][-1])
        else:
            dt_starts.append(df["period"][0])
            dt_ends.append(df["period"][-1])

        # Build list of endpoints for each chunk
        endpoints = []
        for i in range(len(dt_starts)):
            start_str = "&start=" + dt_starts[i].strftime(
                "%Y-%m-%dT%H"
            )  # Format: # 2024-01-01T01
            end_str = "&end=" + dt_ends[i].strftime("%Y-%m-%dT%H")

            endpoints.append(
                self.BASE_URL
                + api_path
                + "?data[]=value"
                + facet_str
                + start_str
                + end_str
                + len_str
                + freq_str
            )

        # Display the number of chunks and the endpoints
        n_chunks = len(endpoints)
        if n_chunks > 1:
            print(
                f"\nNumber of chunks: {n_chunks}\n\nRequesting in parallel the following endpoints:\n"
            )
            for endpoint in endpoints:
                print(endpoint)
        else:
            print("\nRequesting the following endpoint:\n")
            print(endpoints[0])

        return endpoints

    def __format_df_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Format the columns types of the Polars DataFrame.
        """
        df = df.with_columns(
            [pl.col("value").cast(pl.Float64), pl.col("period") + ":00"]
        )
        # Convert the period column to datetime
        df = df.with_columns(
            pl.col("period").str.to_datetime(format="%Y-%m-%dT%H:%M", time_zone="UTC")
        )
        return df.sort("period")

    # Helpers

    def __concat_facets_string(self, facets: dict = None) -> str:
        """Concatenates facet parameters into a URL query string.
        Args:
            selected_facets (dict): Dictionary of facets where keys are facet names and
                values are either strings or lists of strings.
        Returns:
            str: Concatenated facets string for URL query."""

        # facet_str = ""
        # if facets is not None:
        #     # Extract from dictionary
        #     for i in facets.keys():
        #         if type(facets[i]) is list:
        #             for facet in facets[i]:
        #                 # Un-list and concatenate facets strings
        #                 facet_str = facet_str + "&facets[" + i + "][]=" + facet
        #         elif type(facets[i]) is str:
        #             facet_str = facet_str + "&facets[" + i + "][]=" + facets[i]

        facet_str = ""
        if facets:
            # Extract from dictionary
            for facet_name in facets:
                if isinstance(facets[facet_name], list):
                    for facet_value in facets[facet_name]:
                        facet_str += f"&facets[{facet_name}][]={facet_value}"
                elif isinstance(facets[facet_name], str):
                    facet_str += f"&facets[{facet_name}][]={facets[facet_name]}"

        return facet_str

    # ================================================
    # Public Methods
    # ================================================
    def get_eia_hourly_data(
        self,
        api_path: str,
        facets: Optional[dict] = None,
        start: datetime.datetime = None,
        end: datetime.datetime = None,
        max_rows_request: int = 4000,
    ) -> pl.DataFrame:
        """
        This method first extracts the number of time series to be requested using a probing
        endpoint with one hour of data. This depends on the selected facest/filters by the user.
        Then it request chunks in parallel.
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

        # Probe data to check number of time series in the payload
        probe_endpoint = self.__generate_probe_endpoint(api_path, facets, start, end)
        n_ts = self.__probe_data(endpoint_url=probe_endpoint)

        # Generate the [list] of endpoints urls to be requested
        endpoints = self.__generate_endpoint_chunks(
            api_path, facets, start, end, max_rows_request, n_timeseries=n_ts
        )

        # Get the data from the API
        df = self.__get_data_as_df(endpoints)

        # Format the columns and sort the DataFrame
        df = self.__format_df_columns(df)

        # TODO: Add method to store df metadata in a duckdb (e.g. facets, start, end, etc.)
        # df.metadata = {"facets": facets, "start": start, "end": end}

        # self.save_df_as_duckdb(df, path="./data/metadata/eia_metadata.duckdb", table_name="metadata")

        return df

    def save_df_as_duckdb(
        self,
        df: pl.DataFrame,
        path: str = "./data/raw/eia_data.duckdb",
        table_name: str = "eia_data",
    ) -> None:
        """
        Save a Polars DataFrame with the requested EIA data to a DuckDB file.
        Ideal for large dynamic (updatable) dataset and quick data analysis.
        """
        query = f"CREATE TABLE {table_name} AS SELECT * FROM df"
        con = duckdb.connect(path)
        con.execute(query)
        con.close()

        return None
