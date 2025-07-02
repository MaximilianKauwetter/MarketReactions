from datetime import datetime

import pandas as pd

from Entities.BTTUM import BTTUM
from data_managemant.CountryCodes import COUNTRY

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("future.no_silent_downcasting", True)
pd.set_option("display.float_format", "{:>15,.10f}".format)

interval_daily_returns = (datetime(2010, 1, 1), datetime(2025, 1, 1))
check_ret_dates = (datetime(2017, 5, 25), datetime(2017, 6, 9))
check_esg_years = (2010, 2024)

BTTUM(
    country_codes=[
        COUNTRY.BELGIUM,
        # COUNTRY.SPAIN,
        # COUNTRY.GREAT_BRITAIN,
    ],
    interval_daily_returns=interval_daily_returns,
    print_loading=False,
).execute(
    check_ret_dates=check_ret_dates,
    check_esg_years=check_esg_years,
    plot_esg=True,
)

BTTUM(
    country_codes=[
        # COUNTRY.BELGIUM,
        COUNTRY.SPAIN,
        # COUNTRY.GREAT_BRITAIN,
    ],
    interval_daily_returns=interval_daily_returns,
    print_loading=False,
).execute(
    check_ret_dates=check_ret_dates,
    check_esg_years=check_esg_years,
    plot_esg=True,
)

BTTUM(
    country_codes=[
        # COUNTRY.BELGIUM,
        # COUNTRY.SPAIN,
        COUNTRY.GREAT_BRITAIN,
    ],
    interval_daily_returns=interval_daily_returns,
    print_loading=False,
).execute(
    check_ret_dates=check_ret_dates,
    check_esg_years=check_esg_years,
    plot_esg=True,
)

BTTUM(
    country_codes=[
        COUNTRY.BELGIUM,
        COUNTRY.SPAIN,
        COUNTRY.GREAT_BRITAIN,
    ],
    interval_daily_returns=interval_daily_returns,
    print_loading=False,
).execute(
    check_ret_dates=check_ret_dates,
    check_esg_years=check_esg_years,
    plot_esg=True,
)

print("Done")
