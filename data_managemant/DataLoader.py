import pandas as pd
import os
from data_managemant.LSEGDownloader import LSEGDataDownloader
from datetime import datetime as dt
from datetime import timedelta


class DataLoader:
    FOLDER_DATA: str = os.path.dirname(os.path.realpath(__file__)) + r"\..\data"
    FOLDER_DAILY_STOCK: str = os.path.join(FOLDER_DATA, "daily_stock_data")
    PATH_RAW_FIRM_LISTS: str = os.path.join(FOLDER_DATA, "Firm_lists.xlsx")
    PATH_EXTENDED_FIRM_LISTS: str = os.path.join(FOLDER_DATA, "Extended_Firm_lists.xlsx")
    DELISTING_COLS = ["DelistedDate", "ReasonDelisted"]

    def __init__(self):
        # check folders
        if not os.path.exists(DataLoader.FOLDER_DATA):
            os.mkdir(DataLoader.FOLDER_DATA)
        if not os.path.exists(DataLoader.FOLDER_DAILY_STOCK):
            os.mkdir(DataLoader.FOLDER_DAILY_STOCK)

        self.lseg_downloader = LSEGDataDownloader()
        self._extended_firm_lists = None

    def _download_extend_firm_list(self, save_as_file: bool) -> dict[str, pd.DataFrame]:
        print("Download extended firm list")
        if not os.path.exists(DataLoader.PATH_RAW_FIRM_LISTS):
            raise FileNotFoundError(f"File {DataLoader.PATH_RAW_FIRM_LISTS} does not exist! Put the Firm_lists.xlsx file in the data folder!")
        if os.path.exists(DataLoader.PATH_EXTENDED_FIRM_LISTS):
            raise FileExistsError(f"File {DataLoader.PATH_EXTENDED_FIRM_LISTS} already exists! Delete it first!")
        raw_firm_lists = pd.ExcelFile(DataLoader.PATH_RAW_FIRM_LISTS)
        extended_firm_lists = {}
        for sheet_name in raw_firm_lists.sheet_names:
            country_code = sheet_name.removeprefix("Ausgabe_")
            print(f"Processing extention for {country_code}...")
            firm_list = raw_firm_lists.parse(sheet_name, dtype=str)
            firm_list.set_index("Type", drop=False, inplace=True)
            firm_list = pd.merge(
                left=firm_list,
                right=self.lseg_downloader.extended_RIC_from_DSCD(firm_list["Type"].to_list()),
                left_index=True,
                right_index=True,
                how="left",
            )
            extended_firm_lists[country_code] = firm_list
            print(f"Done processing extention for {country_code}!")
        print("Done processing all firm lists!")
        if save_as_file:
            DataLoader._save_extended_firm_list(extended_firm_lists)
        return extended_firm_lists

    @staticmethod
    def _load_extended_firm_list() -> dict[str, pd.DataFrame]:
        print("Load extended firm list")
        if not os.path.exists(DataLoader.PATH_EXTENDED_FIRM_LISTS):
            raise FileNotFoundError(f"File {DataLoader.PATH_EXTENDED_FIRM_LISTS} does not exist!")
        extended_firm_lists_excel = pd.ExcelFile(DataLoader.PATH_EXTENDED_FIRM_LISTS)
        extended_firm_lists = {}
        for country_code in extended_firm_lists_excel.sheet_names:
            try:
                extended_firm_lists[country_code] = extended_firm_lists_excel.parse(country_code, dtype=str)
            except:
                raise Exception(f"Error while loading {country_code}!")
        return extended_firm_lists

    @staticmethod
    def _save_extended_firm_list(extended_firm_list: dict[str, pd.DataFrame]):
        print("Save extended firm list")
        with pd.ExcelWriter(DataLoader.PATH_EXTENDED_FIRM_LISTS) as writer:
            for country_code, firm_list in extended_firm_list.items():
                print(f"Save {country_code}")
                firm_list.to_excel(writer, sheet_name=country_code, index=False)
        print("Save done")

    @staticmethod
    def delisting_included(extended_firm_list: dict[str, pd.DataFrame]) -> bool:
        for country_code, firm_list in extended_firm_list.items():
            for col in DataLoader.DELISTING_COLS:
                if col not in firm_list.columns:
                    return False
        return True

    def _add_delisting(self, extended_firm_list: dict[str, pd.DataFrame], save_as_file=True) -> dict[str, pd.DataFrame]:
        for country_code, firm_list in extended_firm_list.items():
            print(f"Processing delisting for {country_code}...")
            delisting_information = [col for col in DataLoader.DELISTING_COLS if col not in firm_list.columns]
            if 0 < len(delisting_information):
                additional_info = self.lseg_downloader.delisting_data(RIC=firm_list["RIC"].dropna(), delisting_data_cols=delisting_information)
                country_df = pd.merge(left=firm_list, right=additional_info, left_on="RIC", right_index=True, how="left")
                extended_firm_list[country_code] = country_df
            print(f"Done processing delisting for {country_code}!")
        if save_as_file:
            DataLoader._save_extended_firm_list(extended_firm_list)
        return extended_firm_list

    @property
    def extended_firm_list(self) -> dict[str, pd.DataFrame]:
        if self._extended_firm_lists is None:
            if os.path.exists(DataLoader.PATH_EXTENDED_FIRM_LISTS):
                self._extended_firm_lists = DataLoader._load_extended_firm_list()
            else:
                self._extended_firm_lists = self._download_extend_firm_list(save_as_file=True)
            if not DataLoader.delisting_included(self._extended_firm_lists):
                self._extended_firm_lists = self._add_delisting(self._extended_firm_lists, save_as_file=True)
        return self._extended_firm_lists

    def get_daily_returns(self, country_code: str, RIC: str, start_date: dt, end_date: dt, start_return_index: float = 100.0) -> pd.DataFrame | None:
        dir_path = os.path.join(DataLoader.FOLDER_DAILY_STOCK, country_code)
        file_path = os.path.join(dir_path, f"{RIC}.csv")
        save = False
        if os.path.exists(file_path):
            print(f"LOAD {RIC} from {start_date.strftime("%Y-%m-%d")} till {end_date.strftime("%Y-%m-%d")}")
            df = pd.read_csv(
                file_path,
                sep=";",
                decimal=",",
                index_col=None,
                parse_dates=["date"],
            )
            df["total_return"] = pd.to_numeric(df["total_return"], errors="coerce")
            df["return_index"] = pd.to_numeric(df["return_index"], errors="coerce")

            df.set_index("date", drop=False, inplace=True)
            earliest_date = df["date"].min()
            latest_date = df["date"].max()
            if start_date.date() < earliest_date.date() or latest_date.date() < end_date.date():
                dfs = []
                if start_date < earliest_date:
                    df_before = self.lseg_downloader.get_total_return(
                        RIC=RIC,
                        start_date=start_date,
                        end_date=earliest_date - timedelta(days=1),
                    )
                    if df_before is not None:
                        dfs.append(df_before)

                dfs.append(df)
                if latest_date < end_date:
                    df_after = self.lseg_downloader.get_total_return(
                        RIC=RIC,
                        start_date=latest_date + timedelta(days=1),
                        end_date=end_date,
                    )
                    if df_after is not None:
                        dfs.append(df_after)
                df = pd.concat(dfs, axis="index")
                save = True
        else:
            os.makedirs(dir_path, exist_ok=True)
            df = self.lseg_downloader.get_total_return(
                RIC=RIC,
                start_date=start_date,
                end_date=end_date,
            )
            if df is None:
                return None
            save = True
        df["return_index"] = df["total_return"].add(1).cumprod()
        if save:
            df.to_csv(file_path, sep=";", decimal=",", index=False, header=True)
        df = df[df["date"].between(start_date, end_date, inclusive="both")]
        df["return_index"] = df["return_index"] * start_return_index
        df.set_index("date", drop=False, inplace=True)
        return df

    def get_daily_returns_for_countries(
        self,
        countries: list[str],
        start_date: dt,
        end_date: dt,
        start_return_index: float = 100.0,
    ) -> dict[str, dict[str, pd.DataFrame]]:
        countries_dfs: dict[str, dict[str, pd.DataFrame]] = {}
        for country_code in countries:
            country_df = self.extended_firm_list[country_code]
            rics = country_df.loc[
                (~country_df["RIC"].isna())
                & ((country_df["DelistedDate"].isna()) | (start_date < pd.to_datetime(country_df["DelistedDate"], format="%B %Y"))),
                "RIC",
            ]
            country_dfs = {}
            for i, ric in enumerate(rics):
                print(f"{i+1:>4}/{len(rics):>4}: {ric:<30} | ", end="")
                df = self.get_daily_returns(
                    country_code=country_code,
                    RIC=ric,
                    start_date=start_date,
                    end_date=end_date,
                    start_return_index=start_return_index,
                )
                if df is None:
                    print("No data so continue")
                    continue
                country_dfs[ric] = df
            countries_dfs[country_code] = country_dfs
        return countries_dfs
