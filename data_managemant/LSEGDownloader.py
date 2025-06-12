import math
import time

import lseg.data as ld
from dotenv import load_dotenv
import pandas as pd
import os
from datetime import datetime, timedelta
from lseg.data.content import search
from enum import Enum
import numpy as np


class LSEGInterval(Enum):
    TICK = "tick"
    MINUTE = "minute"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"


class LSEGDataDownloader:
    def __init__(self):
        load_dotenv()
        api_key = os.getenv("api_key")
        ldp_login = os.getenv("ldp_login")
        ldp_password = os.getenv("ldp_password")
        self.session = ld.session.platform.Definition(
            signon_control=True,
            app_key=api_key,
            grant=ld.session.platform.GrantPassword(
                username=ldp_login,
                password=ldp_password,
            ),
        ).get_session()
        ld.session.set_default(self.session)

    def is_open(self) -> bool:
        return self.session.open_state is ld.OpenState.Opened

    def is_closed(self) -> bool:
        return self.session.open_state is ld.OpenState.Closed

    def open(self) -> None:
        if self.is_open():
            return None
        print("Open data downloader session")
        self.session.open()
        return None

    @property
    def ld(self):
        if self.is_closed():
            self.open()
        return ld

    def close(self):
        if self.is_closed():
            return None
        print("Close data downloader session")
        ld.close_session()

    def metadata_views(self) -> pd.DataFrame:
        self.open()
        response = search.metadata.Definition(view=search.Views.SEARCH_ALL).get_data()  # Required parameter
        df = response.data.df
        return df

    def metadata(
        self,
        identifier: str,
        select: str | list[str],
        identifier_values: str | list[str] | pd.Series,
        chunk_size: int = 100,
    ) -> pd.DataFrame:
        self.open()
        if isinstance(select, list):
            select = ",".join(select)
        if isinstance(identifier_values, pd.Series):
            terms = identifier_values.to_list()
        elif isinstance(identifier_values, list):
            terms = identifier_values
        elif isinstance(identifier_values, str):
            terms = [identifier_values]
        else:
            raise AttributeError()
        chunks = np.array_split(terms, math.ceil(len(terms) / chunk_size))
        dfs = []
        for i, chunk in enumerate(chunks):
            print(f"\tLoad chunk {i + 1}/{len(chunks)} ", end="")
            dfs.append(
                search.lookup.Definition(
                    view=search.Views.SEARCH_ALL,
                    scope=identifier,
                    terms=",".join(chunk),
                    select=select,
                )
                .get_data()
                .data.df
            )
            print("Done")
        return pd.concat(dfs, axis="index")

    def extended_RIC_from_DSCD(self, DSCD: str | list[str] | pd.Series, chunk_size: int = 100) -> pd.DataFrame:
        return self.metadata(
            identifier="DsQuotationNumber",
            select=[
                "RIC",
                "TickerSymbol",
                "IssuerTicker",
                "IssueValuationQuoteRic",
                "PrimaryRIC",
                "IssuerEquityPrimaryRIC",
                "RICStripped",
                "RicRoot",
                "ShortName",
                "IssuerShortName",
                "MainFilingCommonName",
                "LocalCode",
                "IssueISIN",
                "SnSISIN",
                "AssetState",
                "AssetStateName",
                "OrganisationStatus",
                "ListingStatusName",
                "DTCharacteristics",
                "ListingStatus",
                "AssetCategoryRootName",
                "LocalScheme",
                "RbssSchemeName",
                "RCSAssetCategoryLeaf",
                "RCSAssetCategoryName",
                "IssuerCountry",
                "RCSExchangeCountryLeaf",
                "RCSFilingCountryLeaf",
                "RCSPrimaryListingReportingCountryLeaf",
                "Opol",
                "OldQuoteTickers",
                "OpolName",
                "ExchangeCode",
                "IsMemberOfIndex",
            ],
            identifier_values=DSCD,
            chunk_size=chunk_size,
        )

    def extended_DSCD_from_RIC(self, RIC: str | list[str] | pd.Series, chunk_size: int = 100) -> pd.DataFrame:
        return self.metadata(
            identifier="RIC",
            select=["DsQuotationNumber", "ISIN", "IsActive", "IsCapitalMarketsActive", "ListingDate", "InactiveDate", "StartDate", "EndDate"],
            identifier_values=RIC,
            chunk_size=chunk_size,
        )

    def RIC_from_DSCD(self, DSCD: str | list[str] | pd.Series, chunk_size: int = 100) -> pd.Series:
        return self.metadata(
            identifier="DsQuotationNumber",
            select="RIC",
            identifier_values=DSCD,
            chunk_size=chunk_size,
        )["RIC"]

    def DSCD_from_RIC(self, RIC: str | list[str] | pd.Series, chunk_size: int = 100) -> pd.Series:
        return self.metadata(
            identifier="RIC",
            select="DsQuotationNumber",
            identifier_values=RIC,
            chunk_size=chunk_size,
        )["DsQuotationNumber"]

    def delisting_data(self, RIC: str | list[str] | pd.Series, delisting_data_cols: list[str], chunk_size: int = 100) -> pd.DataFrame:
        return self.metadata(
            identifier="PrimaryRIC",
            select=delisting_data_cols,
            identifier_values=RIC,
            chunk_size=chunk_size,
        )

    def get_data(
        self,
        RIC: list[str],
        fields: list[str],
        func_name: str = "",
    ) -> None | pd.DataFrame:
        if isinstance(RIC, list):
            print(f"Download {func_name} for {RIC} ")
        elif isinstance(RIC, str):
            print(f"Download {func_name} for {RIC:<20}")
            RIC = [RIC]
        else:
            raise AttributeError()
        self.open()
        for i in range(5):
            try:
                df = ld.get_data(
                    universe=RIC,
                    fields=fields,
                )
                return df
            except Exception as e:
                print(f"Error while downloading {RIC} - try {i + 1}/5: {e}")
                time.sleep(0.5)
        print("No useful download")
        return None

    def get_history(
        self,
        RIC: str | list[str],
        fields: list[str],
        start_date: datetime | int | float | str,
        end_date: datetime | int | float | str,
        interval: LSEGInterval = LSEGInterval.DAILY,
        func_name: str = "",
    ) -> None | pd.DataFrame:
        if isinstance(start_date, (int, float)):
            start_date = datetime(start_date, 1, 1)
        if isinstance(start_date, datetime):
            start_date_str = start_date.strftime("%Y-%m-%d")
        elif isinstance(start_date, str):
            start_date_str = start_date
        else:
            raise AttributeError()

        if isinstance(end_date, (int, float)):
            end_date = datetime(end_date, 12, 31)
        if isinstance(end_date, datetime):
            end_date_str = end_date.strftime("%Y-%m-%d")
        elif isinstance(end_date, str):
            end_date_str = end_date
        else:
            raise AttributeError()

        if isinstance(RIC, list):
            print(f"Download {func_name} for {RIC} from {start_date_str} till {end_date_str}")
        elif isinstance(RIC, str):
            print(f"Download {func_name} for {RIC:<20} from {start_date_str} till {end_date_str}")
            RIC = [RIC]
        else:
            raise AttributeError()
        self.open()
        for i in range(5):
            try:
                df = ld.get_history(
                    universe=RIC,
                    fields=fields,
                    interval=interval.value,
                    start=start_date,
                    end=end_date,
                )
                return df
            except Exception as e:
                print(f"Error while downloading {RIC} - try {i + 1}/5: {e}")
                time.sleep(0.5)
        print("No useful download")
        return None

    def get_total_return(
        self,
        RIC: str | list[str],
        start_date: datetime,
        end_date: datetime,
        interval: LSEGInterval = LSEGInterval.DAILY,
    ) -> pd.DataFrame | None:
        df = self.get_history(
            RIC=RIC,
            fields=["TR.TotalReturn"],
            interval=interval,
            start_date=start_date,
            end_date=end_date,
            func_name="Total Return",
        )
        if df is None:
            return None

        print(df)
        df = (
            df.reindex(pd.date_range(start=start_date, end=end_date, freq="D"), fill_value=0)
            .reset_index(drop=False)
            .rename(columns={"index": "date", "Total Return": "total_return"})
        )
        df["date"] = pd.to_datetime(df["date"])
        df["total_return"] = pd.to_numeric(df["total_return"], errors="coerce") / 100
        return df

    def get_over_night_rates(
        self,
        RIC: str | list[str],
        start_date: datetime,
        end_date: datetime,
        interval: LSEGInterval = LSEGInterval.DAILY,
    ) -> pd.DataFrame | None:
        df = self.get_history(
            RIC=RIC,
            fields=["TR.FIXINGVALUE"],
            start_date=start_date,
            end_date=end_date,
            interval=interval,
            func_name="Over Night Rates",
        )
        if df is None:
            return None

        df = (
            df.reindex(pd.date_range(start=start_date, end=end_date, freq="D"), fill_value=0)
            .reset_index(drop=False)
            .rename(
                columns={
                    "index": "date",
                    "Fixing Value": "total_return",
                }
            )
        )
        df["date"] = pd.to_datetime(df["date"])
        divider = {
            LSEGInterval.YEARLY: 1,
            LSEGInterval.QUARTERLY: 4,
            LSEGInterval.MONTHLY: 12,
            LSEGInterval.WEEKLY: 52,
            LSEGInterval.DAILY: 365,
            LSEGInterval.HOURLY: 365 * 24,
            LSEGInterval.MINUTE: 365 * 24 * 60,
        }[interval]
        df["total_return"] = pd.to_numeric(df["total_return"], errors="coerce") / divider
        return df

    def get_index_rates(
        self,
        RIC: str | list[str],
        start_date: datetime,
        end_date: datetime,
        interval: LSEGInterval = LSEGInterval.DAILY,
    ) -> pd.DataFrame | None:
        start_date -= timedelta(days=1)
        df = self.get_history(
            RIC=RIC,
            fields=["TR.PriceClose"],
            interval=interval,
            start_date=start_date,
            end_date=end_date,
            func_name="Index Rates",
        )
        if df is None:
            return None

        df = (
            df.reindex(pd.date_range(start=start_date, end=end_date, freq="D"))
            .ffill()
            .bfill()
            .pct_change()
            .iloc[1:, :]
            .reset_index(drop=False)
            .rename(
                columns={
                    "index": "date",
                    "Price Close": "total_return",
                }
            )
        )
        df["date"] = pd.to_datetime(df["date"])
        df["total_return"] = pd.to_numeric(df["total_return"], errors="coerce")
        return df

    def get_location(
        self,
        RIC: str | list[str],
    ):
        df = self.get_data(
            RIC=RIC,
            fields=[
                "TR.HeadquartersRegionAlt",
                "TR.HeadquartersRegion",
                "TR.UltimateParentCountryHQ",
                "TR.HeadquartersCountry",
                "TR.ImmediateParentCountryHQ",
                "TR.HeadquartersCity",
                "TR.HQStateProvince",
                "TR.HQMinorRegion",
                "TR.ExchangeCountry",
            ],
        )
        return df

    def get_fundamentals(
        self,
        RIC: str | list[str],
    ):
        df = self.get_history(
            RIC=RIC,
            fields=[
                "TR.CompanyMarketCap",
                "TR.F.TotShHoldEq",
                "TR.F.EBIT",
                "TR.F.IntrExpn",
                "TR.F.TotAssets",
            ],
            interval=LSEGInterval.YEARLY,
            start_date="2000",
            end_date="2030",
        )

        df = (
            df.astype("float")
            .ffill()
            .drop_duplicates(keep="last")
            .reindex(pd.date_range(start=datetime(2000, 12, 31), end=datetime(2030, 12, 31), freq="YE"))
            .dropna(axis="rows", how="all")
            .reset_index(drop=False)
            .rename(
                columns={
                    "index": "date",
                    "Company Market Cap": "market_cap",
                    "Total Shareholders' Equity incl Minority Intr & Hybrid Debt": "book_equity",
                    "Earnings before Interest & Taxes (EBIT)": "ebit",
                    "Interest Expense": "int_exp",
                    "Total Assets": "tot_assets",
                }
            )
        )
        df["date"] = pd.to_datetime(df["date"])
        return df

    def get_full_esg_data(
        self,
        RIC: str,
    ) -> pd.DataFrame | None:
        df = self.get_history(
            RIC=RIC,
            fields=[
                "TR.TRESGScore",
                "TR.TRESGCScore",
                "TR.TRESGCControversiesScore",
                "TR.SocialPillarScore",
                "TR.GovernancePillarScore",
                "TR.EnvironmentPillarScore",
                "TR.TRESGResourceUseScore",
                "TR.TRESGEmissionsScore",
                "TR.TRESGInnovationScore",
                "TR.TRESGWorkforceScore",
                "TR.TRESGHumanRightsScore",
                "TR.TRESGCommunityScore",
                "TR.TRESGProductResponsibilityScore",
                "TR.TRESGManagementScore",
                "TR.TRESGShareholdersScore",
                "TR.TRESGCSRStrategyScore",
            ],
            interval=LSEGInterval.YEARLY,
            start_date="2000",
            end_date="2030",
            func_name="ESG DATA",
        )

        df.dropna(axis="index", how="all", inplace=True)
        df = df.reset_index(drop=False)
        df = df.rename(columns=lambda x: str(x).replace(" ", "_").lower())
        df["date"] = pd.to_datetime(df["date"])
        return df

    def get_yield_curve(
        self,
        curve_RIC: str,
        start_date: datetime,
        end_date: datetime,
        interval: LSEGInterval,
    ) -> pd.DataFrame | None:
        self.open()
        RIC = ld.discovery.Chain(curve_RIC).constituents
        curves = self.get_history(
            RIC=RIC,
            fields=["TR.MIDYIELD"],
            start_date=start_date,
            end_date=end_date,
            interval=interval,
            func_name="Yield Curve",
        )
        return curves.transpose()
