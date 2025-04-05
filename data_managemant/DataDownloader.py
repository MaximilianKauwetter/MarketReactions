import lseg.data as ld
from dotenv import load_dotenv
import pandas as pd
import os


class LSEGDataDownloader:
    def __init__(self):
        load_dotenv()
        API_KEY = os.getenv("API_KEY")
        LDP_LOGIN = os.getenv("LDP_LOGIN")
        LDP_PASSWORD = os.getenv("LDP_PASSWORD")
        self.session = ld.session.platform.Definition(
            signon_control=True,
            app_key=API_KEY,
            grant=ld.session.platform.GrantPassword(
                username=LDP_LOGIN,
                password=LDP_PASSWORD,
            ),
        ).get_session()
        ld.session.set_default(self.session)

    def open(self):
        print("Open data downloader session")
        self.session.open()

    @property
    def ld(self):
        if self.session.open_state is ld.OpenState.Closed:
            self.open()
        return ld

    def close(self):
        if self.session.open_state is ld.OpenState.Closed:
            return
        print("Close data downloader session")
        ld.close_session()

    def __del__(self):
        self.close()
