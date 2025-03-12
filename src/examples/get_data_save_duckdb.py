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

    api_key = os.getenv("EIA_API_KEY")
    client = EIAClient(api_key)

    api_path = "electricity/rto/region-sub-ba-data/data/"

    freq = "hourly" # monthly, annual;

    # Subfilter categories
    facets = {
        "parent": "CISO",  # California Independent System Operator
        "subba": "SDGE"  # San Diego Gas & Electric
    }
    
    dt_start = datetime.datetime(2022, 1, 1, 1)
    dt_end = datetime.datetime(2024, 12, 31, 23)

    #df = client.get_eia_data(api_path=api_path, frequency=freq, facets=facets, start=dt_start, end=dt_end)
    df = client.get_eia_data(api_path=api_path, frequency=freq, facets=facets, start=dt_start, end=dt_end, offset=2000)

    print(df)

    client.save_df_as_duckdb(df, path="./data/raw/eia_SDGE_2022_2024.duckdb")
