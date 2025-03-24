import datetime
import os
import sys
import time

# import plotly.express as px
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from eia_client import EIAPolarClient  # This is the NEW Polar client!


def test_new_polar_client_hourly():
    client = EIAPolarClient(os.getenv("EIA_API_KEY"))

    api_path = "electricity/rto/region-sub-ba-data/data/"

    # Subfilter categories
    facets = {"parent": "CISO", "subba": "SDGE"}

    dt_start = datetime.datetime(2020, 1, 1, 1)
    dt_end = datetime.datetime(2025, 1, 1, 1)

    df = client.get_eia_hourly_data(
        api_path=api_path, facets=facets, start=dt_start, end=dt_end
    )

    print(f"{df.height} observations returned")

    # Create a simple line plot
    # fig = px.line(df, x='period', y='value', title='EIA Data Visualisation')
    # fig.show()

    return print(df)


if __name__ == "__main__":
    start_time = time.time()
    test_new_polar_client_hourly()
    end_time = time.time()

    print(f"\nElapsed time: {end_time - start_time} seconds")
