import DatastreamPy as dsweb
from dotenv import load_dotenv
import os
import pandas as pd


class DatastreamDownloader:
    def __init__(self):
        load_dotenv()
        username = os.getenv("DS_ID")
        pwd = os.getenv("LDP_PASSWORD")

        ds = dsweb.DataClient(None, username, pwd)

        start_date = "2022-01-01"
        end_date = "2022-12-31"

        # Retrieve and print out some timeseries data for Apple and Microsoft
        history = ds.get_data(tickers="@AAPL,@MSFT", fields=["P", "VO", "RI"], kind=1, start=start_date, end=end_date, freq="M")
        history.index = pd.date_range(start_date, end_date, freq="M")
        print(history)
