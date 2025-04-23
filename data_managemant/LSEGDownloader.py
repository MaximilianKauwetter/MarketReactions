import math

import lseg.data as ld
from dotenv import load_dotenv
import pandas as pd
import os
from datetime import datetime
from lseg.data.content import search, symbol_conversion
from enum import Enum
from lseg.data.content import historical_pricing
import numpy as np


class LSEGInterval(Enum):
    TICK = "tick"
    MINUTE = "minute"
    HOUR = "hourly"
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

    def get_total_return(
        self,
        RIC: str | list[str],
        start_date: datetime,
        end_date: datetime,
        interval: LSEGInterval = LSEGInterval.DAILY,
    ) -> pd.DataFrame | None:
        self.open()
        print(f"Download {RIC} from {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}")
        if isinstance(RIC, list):
            universe = RIC
        elif isinstance(RIC, str):
            universe = [RIC]
        else:
            raise AttributeError()

        for i in range(5):
            try:
                df = ld.get_history(
                    universe=universe,
                    fields=["TR.TotalReturn"],
                    interval=interval.value,
                    start=start_date,
                    end=end_date,
                )
                break
            except Exception as e:
                print(f"Error while downloading {RIC} - try {i + 1}/5: {e}")
        else:
            return None
        df = (
            df.reindex(pd.date_range(start=start_date, end=end_date, freq="D"), fill_value=0)
            .reset_index(drop=False)
            .rename(columns={"Total Return": "total_return", "index": "date"})
        )
        df["date"] = pd.to_datetime(df["date"])
        df["total_return"] = df["total_return"] / 100

        return df
