import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go

from data_managemant.CountryCodes import COUNTRY


class FileManager:
    FOLDER_DATA: str = os.path.dirname(os.path.realpath(__file__)) + r"\..\data"
    FOLDER_DAILY_STOCK: str = os.path.join(FOLDER_DATA, "daily_stock_data")
    FOLDER_DAILY_RISK_FREE_RETURNS: str = os.path.join(FOLDER_DATA, "daily_risk_free_returns")
    FOLDER_DAILY_MARKET_RETURNS: str = os.path.join(FOLDER_DATA, "daily_market_returns")
    FOLDER_ESG_DATA: str = os.path.join(FOLDER_DATA, "esg_data")
    FOLDER_FUNDAMENTALS: str = os.path.join(FOLDER_DATA, "fundamentals")
    OUTPUT_RESULT_FOLDER: str = os.path.join(FOLDER_DATA, "results")
    PATH_RAW_FIRM_LISTS: str = os.path.join(FOLDER_DATA, "Firm_lists.xlsx")
    PATH_EXTENDED_FIRM_LISTS: str = os.path.join(FOLDER_DATA, "Extended_Firm_lists.xlsx")

    @staticmethod
    def init_folders():
        os.makedirs(FileManager.FOLDER_DATA, exist_ok=True)
        os.makedirs(FileManager.FOLDER_DAILY_STOCK, exist_ok=True)
        os.makedirs(FileManager.FOLDER_ESG_DATA, exist_ok=True)

    @staticmethod
    def load_raw_firm_lists() -> dict[str, pd.DataFrame]:
        print("Load raw firm list")
        if not os.path.exists(FileManager.PATH_RAW_FIRM_LISTS):
            raise FileNotFoundError(f"File {FileManager.PATH_RAW_FIRM_LISTS} does not exist!")
        raw_firm_lists_excel = pd.ExcelFile(FileManager.PATH_RAW_FIRM_LISTS)
        raw_firm_lists: dict[str, pd.DataFrame] = {}
        for sheet_name in raw_firm_lists_excel.sheet_names:
            country_code = sheet_name.removeprefix("Ausgabe_")
            firm_list = raw_firm_lists_excel.parse(sheet_name, dtype=str)
            firm_list.set_index("Type", drop=False, inplace=True)
            raw_firm_lists[country_code] = firm_list
        return raw_firm_lists

    @staticmethod
    def load_extended_firm_list() -> dict[str, pd.DataFrame] | None:
        print("Load extended firm list")
        if not os.path.exists(FileManager.PATH_EXTENDED_FIRM_LISTS):
            return None
        extended_firm_lists_excel = pd.ExcelFile(FileManager.PATH_EXTENDED_FIRM_LISTS)
        extended_firm_lists = {}
        for country_code in extended_firm_lists_excel.sheet_names:
            extended_firm_list = extended_firm_lists_excel.parse(country_code, dtype=str)
            extended_firm_list.set_index("Type", drop=False, inplace=True)
            extended_firm_lists[country_code] = extended_firm_list
        return extended_firm_lists

    @staticmethod
    def save_extended_firm_list(extended_firm_list: dict[str, pd.DataFrame]):
        print("Save extended firm list")
        with pd.ExcelWriter(FileManager.PATH_EXTENDED_FIRM_LISTS) as writer:
            for country_code, firm_list in extended_firm_list.items():
                print(f"Save {country_code}")
                firm_list.to_excel(writer, sheet_name=country_code, index=False)
        print("Save done")

    @staticmethod
    def _read_daily_returns(file_path: str, print_stuff: bool = True) -> tuple[pd.DataFrame | None, datetime | None, datetime | None]:
        if not os.path.exists(file_path):
            print()
            return None, None, None
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
        min_date = df["date"].min()
        max_date = df["date"].max()
        if pd.isna(min_date) or pd.isna(max_date):
            if print_stuff:
                print("empty_date")
            return None, None, None
        if print_stuff:
            print(f"from {min_date.strftime('%Y-%m-%d')} till {max_date.strftime('%Y-%m-%d')}")
        return df, min_date, max_date

    @staticmethod
    def read_daily_stock_returns(
        country_code: COUNTRY, RIC: str, print_stuff: bool = True
    ) -> tuple[pd.DataFrame | None, datetime | None, datetime | None]:
        file_path = os.path.join(FileManager.FOLDER_DAILY_STOCK, country_code.value, f"{RIC}.csv")
        if print_stuff:
            print(f"{country_code.value+":":<4} {RIC:<20} Read Daily Stock Return         ", end="")
        return FileManager._read_daily_returns(file_path, print_stuff=print_stuff)

    @staticmethod
    def read_daily_risk_free_returns(country_code: COUNTRY, print_stuff: bool = True) -> tuple[pd.DataFrame | None, datetime | None, datetime | None]:
        file_path = os.path.join(FileManager.FOLDER_DAILY_RISK_FREE_RETURNS, f"{country_code.value}.csv")
        if print_stuff:
            print(f"{country_code.value+":":<4}                      Read Risk Free Rates            ", end="")
        return FileManager._read_daily_returns(file_path, print_stuff=print_stuff)

    @staticmethod
    def read_daily_market_returns(country_code: COUNTRY, print_stuff: bool = True) -> tuple[pd.DataFrame | None, datetime | None, datetime | None]:
        file_path = os.path.join(FileManager.FOLDER_DAILY_MARKET_RETURNS, f"{country_code.value}.csv")
        if print_stuff:
            print(f"{country_code.value+":":<4}                      Read Market Returns             ", end="")
        return FileManager._read_daily_returns(file_path, print_stuff=print_stuff)

    @staticmethod
    def save_daily_returns(folder_path: str, file_name: str, df: pd.DataFrame):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, file_name)
        df["return_index"] = df["total_return"].add(1).cumprod()
        df.to_csv(file_path, sep=";", decimal=",", index=False, header=True)

    @staticmethod
    def save_daily_stock_returns(country_code: COUNTRY, RIC: str, df: pd.DataFrame):
        folder_path = os.path.join(FileManager.FOLDER_DAILY_STOCK, country_code.value)
        FileManager.save_daily_returns(folder_path=folder_path, file_name=f"{RIC}.csv", df=df)

    @staticmethod
    def save_daily_risk_free_returns(country_code: COUNTRY, df: pd.DataFrame):
        FileManager.save_daily_returns(
            folder_path=FileManager.FOLDER_DAILY_RISK_FREE_RETURNS,
            file_name=f"{country_code.value}.csv",
            df=df,
        )

    @staticmethod
    def save_daily_market_returns(country_code: COUNTRY, df: pd.DataFrame):
        FileManager.save_daily_returns(
            folder_path=FileManager.FOLDER_DAILY_MARKET_RETURNS,
            file_name=f"{country_code.value}.csv",
            df=df,
        )

    @staticmethod
    def load_no_esg_data_list(country_code: COUNTRY) -> list[str]:
        filepath = os.path.join(FileManager.FOLDER_ESG_DATA, country_code.value, "_no_data_list.txt")
        if not os.path.exists(filepath):
            return []
        no_esg_data_list = open(filepath, "r").read().splitlines()
        return no_esg_data_list

    @staticmethod
    def save_no_esg_data_list(country_code: COUNTRY, no_esg_data_list: list[str]):
        dir_folder = os.path.join(FileManager.FOLDER_ESG_DATA, country_code.value)
        if not os.path.exists(dir_folder):
            os.makedirs(dir_folder, exist_ok=True)
        filepath = os.path.join(dir_folder, "_no_data_list.txt")
        open(filepath, "w").write("\n".join(no_esg_data_list))

    @staticmethod
    def read_esg_data(
        country_code: COUNTRY,
        RIC: str,
        print_stuff: bool = True,
    ) -> pd.DataFrame | None:
        file_path = os.path.join(FileManager.FOLDER_ESG_DATA, country_code.value, f"{RIC}.csv")
        if not os.path.exists(file_path):
            return None
        if print_stuff:
            print(f"{country_code.value+":":<4} {RIC:<20} Read ESG                        ", end="")
        df = pd.read_csv(
            file_path,
            sep=";",
            decimal=",",
            index_col=None,
            parse_dates=["date"],
        )
        if len(df) == 0:
            if print_stuff:
                print("empty_date")
            return None
        if print_stuff:
            print()
        return df

    @staticmethod
    def save_esg_data(
        country_code: COUNTRY,
        RIC: str,
        df: pd.DataFrame,
    ):
        dir_folder = os.path.join(FileManager.FOLDER_ESG_DATA, country_code.value)
        if not os.path.exists(dir_folder):
            os.makedirs(dir_folder, exist_ok=True)
        file_path = os.path.join(dir_folder, f"{RIC}.csv")
        df.to_csv(file_path, sep=";", decimal=",", index=False, header=True)

    @staticmethod
    def load_no_fundamentals_list(country_code: COUNTRY) -> list[str]:
        filepath = os.path.join(FileManager.FOLDER_FUNDAMENTALS, country_code.value, "_no_fundamentals_list.txt")
        if not os.path.exists(filepath):
            return []
        no_fundamentals_list = open(filepath, "r").read().splitlines()
        return no_fundamentals_list

    @staticmethod
    def save_no_fundamentals_list(country_code: COUNTRY, no_fundamentals_list: list[str]):
        dir_folder = os.path.join(FileManager.FOLDER_FUNDAMENTALS, country_code.value)
        if not os.path.exists(dir_folder):
            os.makedirs(dir_folder, exist_ok=True)
        filepath = os.path.join(dir_folder, "_no_fundamentals_list.txt")
        open(filepath, "w").write("\n".join(no_fundamentals_list))

    @staticmethod
    def read_fundamentals(
        country_code: COUNTRY,
        RIC: str,
        print_stuff: bool = True,
    ):
        file_path = os.path.join(FileManager.FOLDER_FUNDAMENTALS, country_code.value, f"{RIC}.csv")
        if not os.path.exists(file_path):
            return None
        if print_stuff:
            print(f"{country_code.value+":":<4} {RIC:<20} Read Fundamentals               ", end="")
        df = pd.read_csv(
            file_path,
            sep=";",
            decimal=",",
            index_col=None,
            parse_dates=["date"],
        )
        if len(df) == 0:
            if print_stuff:
                print("empty_date")
            return None
        if print_stuff:
            print()
        return df

    @staticmethod
    def save_fundamentals(
        country_code: COUNTRY,
        RIC: str,
        df: pd.DataFrame,
    ):
        dir_folder = os.path.join(FileManager.FOLDER_FUNDAMENTALS, country_code.value)
        if not os.path.exists(dir_folder):
            os.makedirs(dir_folder, exist_ok=True)
        file_path = os.path.join(dir_folder, f"{RIC}.csv")
        df.to_csv(file_path, sep=";", decimal=",", index=False, header=True)

    @staticmethod
    def write_excel_results(excel_name: str, dfs: dict[..., pd.DataFrame]):
        excel_name = excel_name if "." in excel_name else f"{excel_name}.xlsx"
        output_excel_file_path = os.path.join(FileManager.OUTPUT_RESULT_FOLDER, excel_name)
        os.makedirs(os.path.dirname(output_excel_file_path), exist_ok=True)
        with pd.ExcelWriter(output_excel_file_path, engine="openpyxl") as writer:
            for sheet_name, df in dfs.items():
                if 31 < len(sheet_name):
                    raise ValueError(f'sheet_name "{sheet_name}" must be less than 32 characters, but is {len(sheet_name)}')
                df.to_excel(writer, sheet_name=sheet_name, index=True)

    @staticmethod
    def save_fig(fig: go.Figure, name: str, resolution: tuple[int, int] = (3840, 2160)):
        if "." not in name:
            name = f"{name}.png"
        output_fig_file_path = os.path.join(FileManager.OUTPUT_RESULT_FOLDER, name)
        os.makedirs(os.path.dirname(output_fig_file_path), exist_ok=True)
        for i in range(3):
            try:
                print(f"\tTry Kaleido {i} ", end="")
                fig.write_image(
                    output_fig_file_path,
                    width=resolution[0],
                    height=resolution[1],
                    format="png",
                    engine="kaleido",
                )
                print("Success")
                break
            except:
                print(f"Fail")
                try:
                    print(f"\tTry Orca {i} ", end="")
                    fig.write_image(
                        output_fig_file_path,
                        width=resolution[0],
                        height=resolution[1],
                        format="png",
                        engine="orca",
                    )
                    print("Success")
                    break
                except:
                    print(f"Fail")
