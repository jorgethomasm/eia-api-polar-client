import os
from eia_client import EIAClient


if __name__ == "__main__":
    api_key = os.getenv("EIA_API_KEY")
    client = EIAClient(api_key)
    series_id = "ELECTRICITY_SERIES_ID_HERE"
    df = client.get_electricity_data(series_id)
    print(df)

# Example usage
# if __name__ == "__main__":



