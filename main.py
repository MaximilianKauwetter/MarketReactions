import numpy as np
import pandas as pd
from Entities.BTTUM import BTTUM
from datetime import datetime

from data_managemant.CountryCodes import COUNTRY
from data_managemant.DataLoader import DataLoader
from data_managemant.LSEGDownloader import LSEGDataDownloader, LSEGInterval

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("future.no_silent_downcasting", True)
pd.set_option("display.float_format", "{:>15,.10f}".format)

dl = DataLoader()
print(dl.get_risk_free_rate(country_code=COUNTRY.GREAT_BRITAIN, start_date=datetime(2025, 3, 1), end_date=datetime(2025, 3, 10)))
print(dl.get_market_return(country_code=COUNTRY.GREAT_BRITAIN, start_date=datetime(2025, 1, 1), end_date=datetime(2025, 3, 1)))
print(dl.get_fundamentals(COUNTRY.GERMANY, RIC="alvg.de", start_year=2000, end_year=2030))
# print(df)
# "0#AUBMK=" SONIAOSR=
# downloader = LSEGDataDownloader()

# df = downloader.get_total_return("ALVG.DE", start_date=datetime(2025, 5, 1), end_date=datetime(2025, 5, 10))

# df = downloader.get_over_night_rates(["SONIAOSR="], start_date=datetime(2025, 3, 1), end_date=datetime(2025, 5, 10))
# df = downloader.get_over_night_rates(["EUROSTR="], start_date=datetime(2025, 3, 1), end_date=datetime(2025, 5, 10))

# df = downloader.get_index_rates([".BFXI"], start_date=datetime(2025, 3, 1), end_date=datetime(2025, 5, 10))
# df = downloader.get_index_rates([".IBEXTR"], start_date=datetime(2025, 3, 1), end_date=datetime(2025, 5, 10))
# df = downloader.get_index_rates([".TRIUKX"], start_date=datetime(2025, 3, 1), end_date=datetime(2025, 5, 10))

# print(df)

# Fundamentals to get
# Head quater
# Stock exchange
# Market cap
# Book equity
# operating income
# interestexpense
# Total assets

# "ALVG.DE"

# df = downloader.get_fundamentals(RIC="SHEL.L")
# df = downloader.get_fundamentals(RIC="alvg.de")


# df = downloader.get_yield_curve("AUBB3M", start=datetime(2025, 6, 2), end=datetime(2025, 6, 2), interval=LSEGInterval.DAILY)
# print(df)

exit()

t = downloader.ld.discovery.Chain("0#AUBMK=")
universe = t.constituents
print(universe)
x = (
    downloader.ld.get_history(
        universe=universe,
        fields=[
            "TR.ISMAASKYIELD",
            "TR.SIMPLEYIELD",
            "TR.YIELDTOWORST",
            "TR.HIGHYIELD",
            "TR.OPENYIELD",
            "TR.ASKYIELD",
            "TR.LOWYIELD",
            "TR.MIDYIELD",
            "TR.BIDYIELD",
            "YIELD",
            "TR.FiMaturityStandardYield",
            "TR.YIELDTOMATURITY",
            "TR.YIELDNETCHANGE",
            "TR.YieldToCallAnalytics",
            "TR.YieldToCallDateAnalytics",
        ],
        start="2025-03-31",
        end="2025-03-31",
        interval=LSEGInterval.MONTHLY.value,
    )
    .stack(level=0)
    .sort_index()
)
print(x)
x.to_csv(r"C:\Users\maxim\OneDrive\Dokumente\Python\BT_TuM_MarketReactions\aud_yields.csv", sep=";", index=True, decimal=".")
downloader.close()
exit()
#
# print(np.log(np.exp(1.1)))
# exit()

x = 1000

a = 1.1
b = a**x
c = b ** (1 / x)

m = np.exp(a)
n = m * x
q = np.log(n)
o = n / x
p = np.log(o)

print(a)
print(b)
print("c", c)
print()
print(a)
print(m)
print(n)
print(q)
print(o)
print(p)

exit()

bt_tum = BTTUM(["BE", "ES", "GB"])
# bt_tum.get_daily_returns()
bt_tum.get_esg()


# dl = DataLoader()
# dl.get_esg_data_for_countries(["BE", "ES", "GB"], start_year=2000, end_year=2030, use_dead_list=True)
