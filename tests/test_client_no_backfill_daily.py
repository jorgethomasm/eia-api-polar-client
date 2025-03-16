import datetime
import os
import sys
import plotly.express as px
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from eia_client import EIAClient

def test_client_no_backfill_daily():

    api_key = os.getenv("EIA_API_KEY")
    
   
    client = EIAClient(api_key)

    # API path for DAILY data
    api_path = "electricity/rto/daily-region-sub-ba-data/data/"

    # Parameters
    freq = "daily" 

    # Subfilter categories
    facets = {"parent": "CISO", 
              "subba": "SDGE"}
    dt_start = datetime.date(2024, 1, 1)
    dt_end = datetime.date(2024, 1, 10)

    """"
    "https://api.eia.gov/v2/electricity/rto/daily-region-sub-ba-data/data/?frequency=daily&data[0]=value&facets[subba][]=SDGE&facets[parent][]=CISO&start=2024-01-01&end=2024-01-10&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000
    """
    
    df = client.get_eia_data(api_path=api_path, frequency=freq, facets=facets, start=dt_start, end=dt_end) 
    
    print(f"{df.height} observations returned")

    # Create a simple line plot
    fig = px.line(df, x='period', y='value', title='EIA Data Visualisation')
    fig.show()
    
    return print(df)


if __name__ == "__main__":
    test_client_no_backfill_daily()
    