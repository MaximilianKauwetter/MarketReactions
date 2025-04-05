import pandas as pd
import numpy as np
from data_managemant.DataLoader import DataLoader
from data_managemant.DataDownloader import LSEGDataDownloader
from datetime import date, datetime

# x = LSEGDataDownloader()
DataLoader().load_daily_ts("DE", "ALV", date(2025, 1, 20), date(2025, 1, 25))
