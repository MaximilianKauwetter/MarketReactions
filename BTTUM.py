import pandas as pd
import os

from data_managemant.DataLoader import DataLoader
from datetime import datetime as dt


class BTTUM:
    FOLDER_DATA: str = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")
    PATH_RAW_FIRM_LISTS = os.path.join(FOLDER_DATA, "Firm_lists.xlsx")
    PATH_EXTENDED_FIRM_LISTS = os.path.join(FOLDER_DATA, "Extended_Firm_lists.xlsx")

    def __init__(self):
        self.data_loader = DataLoader()
        self._extended_firm_lists = None

    def get_daily_returns_BE_ES_GB(self):
        self.data_loader.get_daily_returns_for_countries(
            countries=["BE", "ES", "GB"],
            start_date=dt(2016, 1, 1),
            end_date=dt(2025, 4, 1),
        )
