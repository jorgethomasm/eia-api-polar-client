import os
import datetime
from eia_client import EIAClient


# Example usage
if __name__ == "__main__":

    api_key = os.getenv("EIA_API_KEY")
    client = EIAClient(api_key)

    api_path = "rto/region-sub-ba-data/data/"

    freq = "hourly"

    # Categories to slice data
    facets = {
        "parent": "CISO",
        "subba": "SDGE"
    }

    """
    https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/?frequency=hourly&data[0]=value&facets[parent][]=CISO&facets[subba][]=SDGE&start=2025-01-01T00&end=2025-02-28T00&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000
        
    """

    dt_start = datetime.datetime(2024, 1, 1, 1)
    dt_end = datetime.datetime(2024, 1, 25, 23)

    def day_offset(start: datetime.date, end: datetime.date, offset: int) -> list:
        """
        Generate a list of date type elements with the given offset
        representing days. This function is for the back-fill operation,
        given that the maximum request size of the API is of approximate 2500 data points.
        """
        current = [start]
        while max(current) < end:
            if max(current) + datetime.timedelta(days=offset) < end:
                current.append(max(current) + datetime.timedelta(days=offset))
            else:
                current.append(end)
        return current

    d_start = datetime.date(2024, 1, 1)
    d_end = datetime.date(2024, 1, 25)


    def day_offset2(start: datetime.date, end: datetime.date, offset: int) -> list:
        """
        Generate a list of date type elements with the given offset
        representing days. This function is for the back-fill operation,
        given that the maximum request size of the API is of approximate 2500 data points.
        """
        current = [start]
        last_date = start

        while last_date < end:
            next_date = last_date + datetime.timedelta(days=offset)
            if next_date < end:
                current.append(next_date)
                last_date = next_date
            else:
                break
        current.append(end)

        return current

    test_vec = day_offset2(d_start,d_end, 10)

    print(test_vec)
    #df = client.get_electricity_data(api_path=api_path, frequency=freq, facets=facets, start=dt_start, end=dt_end)

    #print(df)
