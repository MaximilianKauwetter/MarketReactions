import pandas as pd
from datetime import date
import os
from data_managemant.DataDownloader import LSEGDataDownloader


class DataLoader:
    FOLDER_DATA: str = os.path.dirname(os.path.realpath(__file__)) + r"\..\data"
    FOLDER_DAILY_STOCK: str = f"{FOLDER_DATA}\daily_stock_data"

    def __init__(self):
        self.data_downloader = LSEGDataDownloader()

    def load_daily_ts(self, country_code: str, code: str, start_date: date, end_date: date) -> pd.DataFrame:
        file_path = os.path.join(DataLoader.FOLDER_DAILY_STOCK, country_code, f"{code}.csv")
        if not os.path.exists(file_path):
            print(f"{file_path} DOES NOT EXIST")
            folder_path = os.path.join(DataLoader.FOLDER_DAILY_STOCK, country_code)
            if not os.path.exists(folder_path):
                print(f"{folder_path} DOES NOT EXIST")
                os.makedirs(folder_path)
        else:
            pass

    def save_daily_ts(self, country_code: str, code: str, start_date: date, end_date: date):
        pass
