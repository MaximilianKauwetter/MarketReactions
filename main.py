import pandas as pd
import numpy as np
from data_managemant.DataLoader import DataLoader
from data_managemant.LSEGDownloader import LSEGDataDownloader, LSEGInterval
from datetime import date, datetime
from lseg.data.content import search, symbol_conversion
from lseg.data.content import historical_pricing
from lseg.data.content.historical_pricing import Intervals
import os
from dotenv import load_dotenv
from data_managemant.DatastreamDownloader import DatastreamDownloader
from BTTUM import BTTUM

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("future.no_silent_downcasting", True)


bt_tum = BTTUM()
bt_tum.get_daily_returns_BE_ES_GB()
