import datetime
import os
import sys
import plotly.express as px
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from eia_client import EIAClient

def test_client_no_backfill_hourly():

    api_key = os.getenv("EIA_API_KEY")
    
   
    client = EIAClient(api_key)

    api_path = "electricity/rto/region-sub-ba-data/data/"

    # Parameters
    freq = "hourly"

    # Subfilter categories
    facets = {"parent": "CISO", 
              "subba": "SDGE"}
    dt_start = datetime.datetime(2024, 1, 1, 1)
    dt_end = datetime.datetime(2024, 1, 10, 23)

    df = client.get_eia_data(api_path=api_path, frequency=freq, facets=facets, start=dt_start, end=dt_end) 
    
    print(f"{df.height} observations returned")

    # Create a simple line plot
    fig = px.line(df, x='period', y='value', title='EIA Data Visualisation')
    fig.show()
    
    return print(df)


if __name__ == "__main__":
    test_client_no_backfill_hourly()
    