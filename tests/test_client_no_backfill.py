from eia_client import EIAClient
import datetime
import os
import sys


 # Add the src directory to the PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

def test_client_no_backfill():

    api_key = os.getenv("EIA_API_KEY")
    
   
    client = EIAClient(api_key)

    api_path = "electricity/rto/region-sub-ba-data/data/"

    # Mock parameters
    freq = "hourly" # monthly, annual;

    # Subfilter categories
    facets = {
        "parent": "CISO",
        "subba": "SDGE"
    }

    dt_start = datetime.datetime(2024, 1, 1, 1)
    dt_end = datetime.datetime(2024, 1, 10, 23)

    df = client.get_eia_data(api_path=api_path, frequency=freq, facets=facets, start=dt_start, end=dt_end) 
    
    return print(df)