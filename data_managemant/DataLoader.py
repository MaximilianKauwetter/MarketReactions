from datetime import datetime, timedelta

import pandas as pd

from Entities.Firm import Firm
from data_managemant.CountryCodes import COUNTRY
from data_managemant.FileManager import FileManager
from data_managemant.FirmLists import FirmLists
from data_managemant.LSEGDownloader import LSEGDataDownloader


class DataLoader:
    RF_RATES: dict[COUNTRY, str] = {
        COUNTRY.BELGIUM: "EURIBORSWD=",
        COUNTRY.SPAIN: "EURIBORSWD=",
        COUNTRY.GREAT_BRITAIN: "SONIAOSR=",
    }
    MARKET_RATES: dict[COUNTRY, str] = {
        COUNTRY.BELGIUM: ".BFXI",
        COUNTRY.SPAIN: ".IBEXTR",
        COUNTRY.GREAT_BRITAIN: ".TRIUKX",
    }

    def __init__(self, print_stuff: bool = True):
        # check folders
        self.print_stuff = print_stuff
        FileManager.init_folders()

        self.lseg_downloader = LSEGDataDownloader()
        self.firm_lists = FirmLists(self.lseg_downloader)
        self._no_esg_data_lists: dict[COUNTRY, list[str]] = {}
        self._no_fundamentals_lists: dict[COUNTRY, list[str]] = {}
        self._firms: dict[COUNTRY, dict[str, dict[int, Firm]]] = {}
        self._rf_cache: dict[COUNTRY, dict[int, pd.DataFrame]] = {}
        self._mr_cache: dict[COUNTRY, dict[int, pd.DataFrame]] = {}

    @staticmethod
    def delisting_year_from_ric(ric: str) -> None | tuple[str, int]:
        if "^" not in ric:
            return None
        ric, year = ric.split("^", 1)
        year = int(year[1:])
        if 30 < year:
            year = 1900 + year
        else:
            year = 2000 + year
        return ric, year

    def get_daily_returns(
        self,
        country_code: COUNTRY,
        RIC: str,
        start_date: datetime,
        end_date: datetime,
        start_return_index: float = 100.0,
    ) -> pd.DataFrame | None:
        df, min_date, max_date = FileManager.read_daily_stock_returns(country_code=country_code, RIC=RIC, print_stuff=self.print_stuff)
        save = False
        if df is None or min_date is None or max_date is None:
            df = self.lseg_downloader.get_total_return(
                RIC=RIC,
                start_date=start_date,
                end_date=end_date,
            )
            if df is None or len(df) == 0:
                return None
            min_date = df["date"].min()
            max_date = df["date"].max()
            save = True
        if start_date.date() < min_date.date() or max_date.date() < end_date.date():
            dfs = []
            if start_date.date() < min_date.date():
                df_before = self.lseg_downloader.get_total_return(
                    RIC=RIC,
                    start_date=start_date,
                    end_date=min_date - timedelta(days=1),
                )
                if df_before is not None:
                    dfs.append(df_before)
            dfs.append(df)
            if max_date.date() < end_date.date():
                df_after = self.lseg_downloader.get_total_return(
                    RIC=RIC,
                    start_date=max_date + timedelta(days=1),
                    end_date=end_date,
                )
                if df_after is not None:
                    dfs.append(df_after)
            if 1 < len(dfs):
                df = pd.concat(dfs, axis="index")
                save = True
        df.set_index("date", drop=False, inplace=True)
        if save:
            FileManager.save_daily_stock_returns(country_code=country_code, RIC=RIC, df=df)
        df = df[df["date"].between(start_date, end_date, inclusive="both")].copy()
        df.loc[:, "return_cumulative"] = df["total_return"].add(1).cumprod()
        df.loc[:, "return_index"] = df["return_cumulative"] * start_return_index
        return df

    def get_daily_stock_returns(
        self,
        country_code: COUNTRY,
        RIC: str,
        start_date: datetime,
        end_date: datetime,
        start_return_index: float = 100.0,
    ) -> pd.DataFrame | None:
        return self.get_daily_returns(
            country_code=country_code,
            RIC=RIC,
            start_date=start_date,
            end_date=end_date,
            start_return_index=start_return_index,
        )

    def get_risk_free_rate(
        self,
        country_code: COUNTRY,
        start_date: datetime,
        end_date: datetime,
        start_return_index: float = 100.0,
    ) -> pd.DataFrame | None:
        attribute_hash = hash((start_date, end_date, start_return_index))
        look_up = self._rf_cache.get(country_code, {}).get(attribute_hash, None)
        if look_up is not None:
            return look_up.copy()
        df, min_date, max_date = FileManager.read_daily_risk_free_returns(country_code=country_code, print_stuff=self.print_stuff)
        save = False
        if df is None or min_date is None or max_date is None:
            df = self.lseg_downloader.get_over_night_rates(
                RIC=DataLoader.RF_RATES[country_code],
                start_date=start_date,
                end_date=end_date,
            )
            if df is None or len(df) == 0:
                return None
            min_date = df["date"].min()
            max_date = df["date"].max()
            save = True
        if start_date.date() < min_date.date() or max_date.date() < end_date.date():
            dfs = []
            if start_date.date() < min_date.date():
                df_before = self.lseg_downloader.get_over_night_rates(
                    RIC=DataLoader.RF_RATES[country_code],
                    start_date=start_date,
                    end_date=min_date - timedelta(days=1),
                )
                if df_before is not None:
                    dfs.append(df_before)
            dfs.append(df)
            if max_date.date() < end_date.date():
                df_after = self.lseg_downloader.get_over_night_rates(
                    RIC=DataLoader.RF_RATES[country_code],
                    start_date=max_date + timedelta(days=1),
                    end_date=end_date,
                )
                if df_after is not None:
                    dfs.append(df_after)
            if 1 < len(dfs):
                df = pd.concat(dfs, axis="index")
                save = True
        df.set_index("date", drop=False, inplace=True)
        if save:
            FileManager.save_daily_risk_free_returns(country_code=country_code, df=df)
        df = df[df["date"].between(start_date, end_date, inclusive="both")]
        df["return_cumulative"] = df["total_return"].add(1).cumprod()
        df["return_index"] = df["return_cumulative"] * start_return_index
        if self._rf_cache.get(country_code, None) is None:
            self._rf_cache[country_code] = {}
        self._rf_cache[country_code][attribute_hash] = df.copy()
        return df

    def get_market_return(
        self,
        country_code: COUNTRY,
        start_date: datetime,
        end_date: datetime,
        start_return_index: float = 100.0,
    ) -> pd.DataFrame | None:
        attribute_hash = hash((start_date, end_date, start_return_index))
        look_up = self._mr_cache.get(country_code, {}).get(attribute_hash, None)
        if look_up is not None:
            return look_up.copy()
        df, min_date, max_date = FileManager.read_daily_market_returns(country_code=country_code, print_stuff=self.print_stuff)
        save = False
        if df is None or min_date is None or max_date is None:
            df = self.lseg_downloader.get_index_rates(
                RIC=DataLoader.MARKET_RATES[country_code],
                start_date=start_date,
                end_date=end_date,
            )
            if df is None or len(df) == 0:
                return None
            min_date = df["date"].min()
            max_date = df["date"].max()
            save = True
        if start_date.date() < min_date.date() or max_date.date() < end_date.date():
            dfs = []
            if start_date.date() < min_date.date():
                df_before = self.lseg_downloader.get_index_rates(
                    RIC=DataLoader.MARKET_RATES[country_code],
                    start_date=start_date,
                    end_date=min_date - timedelta(days=1),
                )
                if df_before is not None:
                    dfs.append(df_before)
            dfs.append(df)
            if max_date.date() < end_date.date():
                df_after = self.lseg_downloader.get_index_rates(
                    RIC=DataLoader.MARKET_RATES[country_code],
                    start_date=max_date + timedelta(days=1),
                    end_date=end_date,
                )
                if df_after is not None:
                    dfs.append(df_after)
            if 1 < len(dfs):
                df = pd.concat(dfs, axis="index")
                save = True
        df.set_index("date", drop=False, inplace=True)
        if save:
            FileManager.save_daily_market_returns(country_code=country_code, df=df)
        df = df[df["date"].between(start_date, end_date, inclusive="both")]
        df["return_cumulative"] = df["total_return"].add(1).cumprod()
        df["return_index"] = df["return_cumulative"] * start_return_index
        if self._mr_cache.get(country_code, None) is None:
            self._mr_cache[country_code] = {}
        self._mr_cache[country_code][attribute_hash] = df.copy()
        return df

    def get_daily_stock_returns_for_countries(
        self,
        countries: list[COUNTRY],
        start_date: datetime,
        end_date: datetime,
        start_return_index: float = 100.0,
        use_dead_list: bool = False,
    ) -> dict[COUNTRY, dict[str, pd.DataFrame]]:
        countries_dfs: dict[COUNTRY, dict[str, pd.DataFrame]] = {}
        for country_code in countries:
            country_firm_rics = self.firm_lists.get_county_firm_rics_without_dead_firms(
                country=country_code,
                dead_date=start_date,
                use_dead_list=use_dead_list,
            ).to_list()
            country_dfs = {}
            for i, ric in enumerate(country_firm_rics):
                if self.print_stuff:
                    print(f"{i+1:>4}/{len(country_firm_rics):>4}: {ric:<30} | ", end="")
                df = self.get_daily_stock_returns(
                    country_code=country_code,
                    RIC=ric,
                    start_date=start_date,
                    end_date=end_date,
                    start_return_index=start_return_index,
                )
                if df is None:
                    if self.print_stuff:
                        print("No data so continue")
                    continue
                country_dfs[ric] = df
            countries_dfs[country_code] = country_dfs
        return countries_dfs

    def get_no_esg_data_list(
        self,
        country_code: COUNTRY,
    ) -> list[str]:
        if self._no_esg_data_lists.get(country_code, None) is None:
            self._no_esg_data_lists[country_code] = FileManager.load_no_esg_data_list(country_code)
        return self._no_esg_data_lists[country_code]

    def add_to_no_esg_data_list(
        self,
        country_code: COUNTRY,
        no_esg_data_firm: str,
    ):
        self.get_no_esg_data_list(country_code)
        self._no_esg_data_lists[country_code].append(no_esg_data_firm)
        FileManager.save_no_esg_data_list(country_code, self._no_esg_data_lists[country_code])

    def get_esg_data(
        self,
        country_code: COUNTRY,
        RIC: str,
        start_year: int,
        end_year: int,
    ) -> pd.DataFrame | None:
        if RIC in self.get_no_esg_data_list(country_code=country_code):
            if self.print_stuff:
                print(f"{country_code.value+":":<4} {RIC:<20} No ESG through list")
            return None
        df = FileManager.read_esg_data(country_code=country_code, RIC=RIC, print_stuff=self.print_stuff)
        if df is None:
            df = self.lseg_downloader.get_full_esg_data(RIC=RIC)
            if df is None:
                return None
            if len(df) == 0:
                self.add_to_no_esg_data_list(country_code=country_code, no_esg_data_firm=RIC)
                return None
            FileManager.save_esg_data(country_code=country_code, RIC=RIC, df=df)
        start_date, end_date = datetime(year=start_year, month=1, day=1), datetime(year=end_year, month=12, day=31)
        df = df[df["date"].between(start_date, end_date, inclusive="both")]
        year_end_dates = pd.date_range(datetime(min(df["date"]).year, 12, 31), datetime(max(df["date"]).year, 12, 31), freq="YE")
        df = pd.merge(left=pd.Series(data=year_end_dates, name="date"), right=df, how="outer", on="date").ffill().bfill()
        return df

    def get_esg_data_for_countries(
        self,
        countries: list[COUNTRY],
        start_year: int,
        end_year: int,
        use_dead_list: bool = False,
    ) -> dict[COUNTRY, dict[str, pd.DataFrame]]:
        countries_dfs: dict[COUNTRY, dict[str, pd.DataFrame]] = {}
        for country_code in countries:
            country_firm_rics = self.firm_lists.get_county_firm_rics_without_dead_firms(
                country=country_code,
                dead_date=datetime(year=start_year, month=1, day=1),
                use_dead_list=use_dead_list,
            ).to_list()
            country_dfs = {}
            for i, ric in enumerate(country_firm_rics):
                if self.print_stuff:
                    print(f"{i + 1:>4}/{len(country_firm_rics):>4}: {ric:<30} | ", end="")
                df = self.get_esg_data(
                    country_code=country_code,
                    RIC=ric,
                    start_year=start_year,
                    end_year=end_year,
                )
                if df is None:
                    if self.print_stuff:
                        print("No data so continue")
                    continue
                country_dfs[ric] = df
            countries_dfs[country_code] = country_dfs
        return countries_dfs

    def get_no_fundamentals_list(
        self,
        country_code: COUNTRY,
    ) -> list[str]:
        if self._no_fundamentals_lists.get(country_code, None) is None:
            self._no_fundamentals_lists[country_code] = FileManager.load_no_fundamentals_list(country_code)
        return self._no_fundamentals_lists[country_code]

    def add_to_no_fundamentals_list(
        self,
        country_code: COUNTRY,
        no_fundamentals_firm: str,
    ):
        self.get_no_fundamentals_list(country_code)
        self._no_fundamentals_lists[country_code].append(no_fundamentals_firm)
        FileManager.save_no_fundamentals_list(country_code, self._no_fundamentals_lists[country_code])

    def get_fundamentals(
        self,
        country_code: COUNTRY,
        RIC: str,
        start_year: int,
        end_year: int,
    ) -> pd.DataFrame | None:
        if RIC in self.get_no_fundamentals_list(country_code=country_code):
            if self.print_stuff:
                print(f"{country_code.value+":":<4} {RIC:<20} No Fundamentals through list")
            return None
        df = FileManager.read_fundamentals(country_code=country_code, RIC=RIC, print_stuff=self.print_stuff)
        if df is None:
            df = self.lseg_downloader.get_fundamentals(RIC=RIC)
            if df is None:
                return None
            if len(df) == 0:
                self.add_to_no_fundamentals_list(country_code=country_code, no_fundamentals_firm=RIC)
                return None
            FileManager.save_fundamentals(country_code=country_code, RIC=RIC, df=df)
        start_date, end_date = datetime(year=start_year, month=1, day=1), datetime(year=end_year, month=12, day=31)
        df = df[df["date"].between(start_date, end_date, inclusive="both")]
        return df

    def get_fundamentals_for_countries(
        self,
        countries: list[COUNTRY],
        start_year: int,
        end_year: int,
        use_dead_list: bool = False,
    ) -> dict[COUNTRY, dict[str, pd.DataFrame]]:
        countries_dfs: dict[COUNTRY, dict[str, pd.DataFrame]] = {}
        for country_code in countries:
            country_firm_rics = self.firm_lists.get_county_firm_rics_without_dead_firms(
                country=country_code,
                dead_date=datetime(year=start_year, month=1, day=1),
                use_dead_list=use_dead_list,
            ).to_list()
            country_dfs = {}
            for i, ric in enumerate(country_firm_rics):
                if self.print_stuff:
                    print(f"{i + 1:>4}/{len(country_firm_rics):>4}: {ric:<30} | ", end="")
                df = self.get_fundamentals(
                    country_code=country_code,
                    RIC=ric,
                    start_year=start_year,
                    end_year=end_year,
                )
                if df is None:
                    if self.print_stuff:
                        print("No data so continue")
                    continue
                country_dfs[ric] = df
            countries_dfs[country_code] = country_dfs
        return countries_dfs

    def create_firm(
        self,
        country_code: COUNTRY,
        RIC: str,
        interval_daily_returns: tuple[datetime, datetime],
        interval_esg: tuple[int, int],
        min_num_days: int | float = None,
    ) -> Firm:
        if min_num_days is not None and min_num_days < 0:
            raise AttributeError("min_num_dates cannot be negative")
        meta = self.firm_lists.extended_firm_lists[country_code.value].set_index("RIC", drop=False).loc[RIC, :]
        fundamentals: None | pd.DataFrame = self.get_fundamentals(
            country_code=country_code,
            RIC=RIC,
            start_year=min(interval_daily_returns).year,
            end_year=max(interval_daily_returns).year,
        )
        daily_returns = self.get_daily_stock_returns(
            country_code=country_code,
            RIC=RIC,
            start_date=min(interval_daily_returns),
            end_date=max(interval_daily_returns),
        )
        esg_data: None | pd.DataFrame = self.get_esg_data(
            country_code=country_code,
            RIC=RIC,
            start_year=min(interval_esg),
            end_year=max(interval_esg),
        )
        risk_free_rate = self.get_risk_free_rate(
            country_code=country_code,
            start_date=min(interval_daily_returns),
            end_date=max(interval_daily_returns),
        )["total_return"]
        market_return = self.get_market_return(
            country_code=country_code,
            start_date=min(interval_daily_returns),
            end_date=max(interval_daily_returns),
        )["total_return"]

        if fundamentals is not None:
            fundamentals = fundamentals.dropna(axis="rows", how="any")
            if len(fundamentals) < 2:
                fundamentals = None
        if self.print_stuff and fundamentals is None:
            print(f"{country_code.value + ":":<4} {RIC:<20} NOT ENOUGH FUNDAMENTALS")

        num_days = None
        if daily_returns is not None:
            tr = daily_returns["total_return"].dropna()
            non_zero = ~daily_returns["total_return"].eq(0)
            if non_zero.any():
                last_nonzero_idx = daily_returns.index.get_loc(non_zero[::-1].idxmax())
                daily_returns = daily_returns.iloc[: last_nonzero_idx + 1, :]
            else:
                daily_returns = None
            num_days = len(tr[~tr.eq(0)])
            if min_num_days is not None and 0 < min_num_days < 1:
                min_num_days = (max(interval_daily_returns) - min(interval_daily_returns)).days * min_num_days
            if min_num_days is not None and num_days < min_num_days:
                daily_returns = None
        if self.print_stuff and daily_returns is None:
            exp = "None" if min_num_days is None else f">{min_num_days:<7.2f}"
            print(f"{country_code.value + ":":<4} {RIC:<20} TO LESS DAYS FOR DAILY RETURNS Actual:{num_days:<7.2f} Expected: {exp}")

        if self.print_stuff and esg_data is None:
            print(f"{country_code.value + ":":<4} {RIC:<20} NO ESG")

        if self.print_stuff and fundamentals is not None and daily_returns is not None and esg_data is not None:
            print(f"{country_code.value + ":":<4} {RIC:<20} SUCCESS")

        if self.print_stuff:
            print()

        return Firm(
            meta=meta,
            fundamentals=fundamentals,
            df_daily_returns=daily_returns,
            df_esg=esg_data,
            risk_free_rate=risk_free_rate,
            market_returns=market_return,
        )

    def get_firm(
        self,
        country_code: COUNTRY,
        RIC: str,
        interval_daily_returns: tuple[datetime, datetime],
        interval_esg: tuple[int, int],
        min_num_days: int | float = None,
    ) -> Firm:
        attribute_hash = hash((interval_daily_returns, interval_esg, min_num_days))
        firm = self._firms.get(country_code, {}).get(RIC, {}).get(attribute_hash, None)
        if firm is None:
            firm = self.create_firm(
                country_code=country_code,
                RIC=RIC,
                interval_daily_returns=interval_daily_returns,
                interval_esg=interval_esg,
                min_num_days=min_num_days,
            )
            if self._firms.get(country_code, None) is None:
                self._firms[country_code] = {}
            if self._firms.get(country_code, None).get(RIC, None) is None:
                self._firms[country_code][RIC] = {}
            self._firms[country_code][RIC][attribute_hash] = firm
        return firm
