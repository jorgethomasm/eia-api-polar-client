import os
import datetime
from eia_client import EIAClient


# Example usage
if __name__ == "__main__":

    api_key = os.getenv("EIA_API_KEY")
    client = EIAClient(api_key)

    freq = "hourly"
    facets = {
        "parent": "CISO",
        "subba": "SDGE"
    }

    """
    https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/?frequency=hourly&data[0]=value&facets[parent][]=CISO&facets[subba][]=SDGE&start=2025-01-01T00&end=2025-02-28T00&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000
    
    
    https://www.eia.gov/opendata/browser/electricity/rto/region-sub-ba-data?frequency=hourly&data=value;&facets=parent;subba;&parent=CISO;&subba=SDGE;&start=2025-01-01T00&end=2025-02-28T00&sortColumn=period;&sortDirection=desc;
    """

    dt_start = datetime.datetime(2024, 1, 1, 1)
    dt_end = datetime.datetime(2024, 1, 31, 23)
    series_id = "ELECTRICITY_SERIES_ID_HERE"

    df = client.get_electricity_data(api_key=api_key,
                                     api_path=api_path,
                                     frequency=freq,
                                     facets=facets,
                                     start=dt_start,
                                     end=dt_end)

    print(df)
