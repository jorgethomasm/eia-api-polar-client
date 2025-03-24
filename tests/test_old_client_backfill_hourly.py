import datetime
import os
import sys
import time

# import plotly.express as px
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from eia_client import EIAClient  # This is the OLD client!


def test_old_client_backfill_hourly():
    client = EIAClient(os.getenv("EIA_API_KEY"))

    api_path = "electricity/rto/region-sub-ba-data/data/"

    # Parameters
    freq = "hourly"

    # Subfilter categories
    facets = {"parent": "CISO", "subba": "SDGE"}

    dt_start = datetime.datetime(2024, 1, 1, 1)
    dt_end = datetime.datetime(2025, 1, 31, 23)

    df = client.get_eia_data(
        api_path=api_path,
        frequency=freq,
        facets=facets,
        start=dt_start,
        end=dt_end,
        offset=4000,
    )

    # print(f"{df.height} observations returned")

    # Create a simple line plot
    # fig = px.line(df, x='period', y='value', title='EIA Data Visualisation')
    # fig.show()

    return print(df)


if __name__ == "__main__":
    start_time = time.time()
    test_old_client_backfill_hourly()
    end_time = time.time

    print("Elapsed time:", end_time - start_time, "seconds")
