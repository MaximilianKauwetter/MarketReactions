import pandas as pd
from datetime import datetime

from data_managemant.CountryCodes import COUNTRY
from data_managemant.FileManager import FileManager


class FirmLists:
    DELISTING_COLS = ["DelistedDate", "ReasonDelisted"]

    def __init__(self, downloader):
        self.lseg_downloader = downloader
        self._raw_firm_lists = None
        self._extended_firm_lists = None
        self._clean_firm_lists = None

    @property
    def raw_firm_lists(self) -> dict[str, pd.DataFrame]:
        if self._raw_firm_lists is None:
            self._raw_firm_lists = FileManager.load_raw_firm_lists()
        return self._raw_firm_lists

    @property
    def extended_firm_lists(self) -> dict[str, pd.DataFrame]:
        if self._extended_firm_lists is None:
            extended_firm_lists = FileManager.load_extended_firm_list()
            if extended_firm_lists is None:
                extended_firm_lists = self.create_extend_firm_list(save_as_file=True)
            self._extended_firm_lists = extended_firm_lists
            if not FirmLists.delisting_included(self._extended_firm_lists):
                self._extended_firm_lists = self._add_delisting(self._extended_firm_lists, save_as_file=True)

        return self._extended_firm_lists

    @property
    def clean_firm_lists(self) -> dict[str, pd.DataFrame]:
        if self._clean_firm_lists is None:
            self._clean_firm_lists = {}
            for country_code, firm_list in self.extended_firm_lists.items():
                self._clean_firm_lists[country_code] = firm_list[~firm_list["RIC"].isna()].copy()
        return self._clean_firm_lists

    def create_extend_firm_list(self, save_as_file: bool) -> dict[str, pd.DataFrame]:
        print("Create extended firm list")
        raw_firm_lists = FileManager.load_raw_firm_lists()
        extended_firm_lists = {}
        for country_code, firm_list in raw_firm_lists.items():
            print(f"Processing extension for {country_code}...")
            extended_firm_list = pd.merge(
                left=firm_list,
                right=self.lseg_downloader.extended_RIC_from_DSCD(firm_list["Type"].to_list()),
                left_index=True,
                right_index=True,
                how="left",
            )
            extended_firm_lists[country_code] = extended_firm_list
            print(f"Done processing extension for {country_code}!")
        print("Done processing all firm lists!")
        if save_as_file:
            FileManager.save_extended_firm_list(extended_firm_lists)
        return extended_firm_lists

    @staticmethod
    def delisting_included(extended_firm_list: dict[str, pd.DataFrame]) -> bool:
        for country_code, firm_list in extended_firm_list.items():
            for col in FirmLists.DELISTING_COLS:
                if col not in firm_list.columns:
                    return False
        return True

    def _add_delisting(self, extended_firm_list: dict[str, pd.DataFrame], save_as_file=True) -> dict[str, pd.DataFrame]:
        for country_code, firm_list in extended_firm_list.items():
            delisting_information = [col for col in FirmLists.DELISTING_COLS if col not in firm_list.columns]
            if 0 < len(delisting_information):
                additional_info = self.lseg_downloader.delisting_data(RIC=firm_list["RIC"].dropna(), delisting_data_cols=delisting_information)
                country_df = pd.merge(left=firm_list, right=additional_info, left_on="RIC", right_index=True, how="left")
                extended_firm_list[country_code] = country_df
        if save_as_file:
            FileManager.save_extended_firm_list(extended_firm_list)
        return extended_firm_list

    def get_county_firm_rics_without_dead_firms(self, country: COUNTRY, dead_date: datetime, use_dead_list: bool = False) -> pd.Series:
        country_df = self.clean_firm_lists[country.value]
        if use_dead_list and "DEAD DATE" in country_df.columns:
            col = "DEAD DATE"
            col_format = None
        else:
            col = "DelistedDate"
            col_format = "%B %Y"
        country_df = country_df[country_df[col].isna() | (dead_date < pd.to_datetime(country_df[col], format=col_format))].reset_index(drop=True)
        return country_df["RIC"]
