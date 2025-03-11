
import datetime
import os
import sys
import plotly.express as px
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from eia_client import EIAClient

def test_client_backfill():

    api_key = os.getenv("EIA_API_KEY")
    
   
    client = EIAClient(api_key)

    api_path = "electricity/rto/region-sub-ba-data/data/"

    # Parameters
    freq = "hourly" # monthly, annual;

    # Subfilter categories
    facets = {"parent": "CISO", "subba": "SDGE"}
    dt_start = datetime.datetime(2023, 1, 1, 1)
    dt_end = datetime.datetime(2025, 1, 31, 23)

    df = client.get_eia_data(api_path=api_path, frequency=freq, facets=facets, start=dt_start, end=dt_end, offset=2000) 
    
    print(f"{df.height} observations returned")

    # Create a simple line plot
    fig = px.line(df, x='period', y='value', title='EIA Data Visualisation')
    fig.show()
    
    return print(df)

test_client_backfill()