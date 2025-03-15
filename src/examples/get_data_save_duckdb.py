import os
import sys
import datetime
# Add the src directory to the PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
from eia_client import EIAClient


# Example usage
if __name__ == "__main__":

    """
    Vocabulary
    route: e.g. electricity
    rto: real-time grid monitor
    """  
    client = EIAClient(api_key=os.getenv("EIA_API_KEY"))

    api_path = "electricity/rto/region-sub-ba-data/data/"

    freq = "hourly" #  or monthly;

    # Subfilter categories
    facets = {
        "parent": "CISO",  # California Independent System Operator
        "subba": "SDGE"  # San Diego Gas & Electric
    }
    
    dt_start = datetime.datetime(2020, 1, 1, 0)
    dt_end = datetime.datetime(2025, 1, 1, 0)

    # No Back-filling
    #df = client.get_eia_data(api_path=api_path, frequency=freq, facets=facets, start=dt_start, end=dt_end)
    
    # Back-filling (large dataset request)
    df = client.get_eia_data(api_path=api_path, frequency=freq, facets=facets, start=dt_start, end=dt_end, offset=2000)
    print(df)

    # Save the data to a DuckDB file
    client.save_df_as_duckdb(df, path="./data/raw/eia_SDGE_2024_demo.duckdb", table_name="eia_data")

    # With Polars is super easy to save the data to a Parquet file
    # Ideal for static (not updatable) datasets
    df.write_parquet("./data/raw/eia_SDGE_2020_2024_demo.parquet")
    