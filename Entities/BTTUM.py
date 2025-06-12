import pandas as pd
import os

from Entities.Country import Country
from data_managemant.DataLoader import DataLoader
from datetime import datetime


class BTTUM:

    def __init__(
        self,
        country_codes: list[str],
        interval_daily_returns: tuple[datetime, datetime],
        interval_esg: tuple[int, int],
        use_dead_list: bool = False,
    ):
        self.data_loader = DataLoader()
        self.countries = {}
        for country_code in country_codes:
            self.countries[country_code] = Country(
                data_loader=self.data_loader,
                country_code=country_code,
                interval_daily_returns=interval_daily_returns,
                interval_esg=interval_esg,
                use_dead_list=use_dead_list,
            )

    def get_daily_returns(self):
        self.data_loader.get_daily_stock_returns_for_countries(
            countries=list(self.countries.keys()),
            start_date=datetime(2016, 1, 1),
            end_date=datetime(2025, 4, 1),
        )

    def get_esg(self):
        self.data_loader.get_esg_data_for_countries(
            countries=list(self.countries.keys()),
            start_year=2005,
            end_year=2030,
            use_dead_list=True,
        )
