from datetime import datetime
from Entities.Firm import Firm
from Entities.FirmSelection import FirmSelection
from data_managemant.DataLoader import DataLoader
from data_managemant.CountryCodes import COUNTRY


class Country(FirmSelection):
    def __init__(
        self,
        data_loader: DataLoader,
        country_code: COUNTRY,
        interval_daily_returns: tuple[datetime, datetime] | None,
        interval_esg: tuple[int, int] | None,
        use_dead_list: bool = False,
    ):
        super().__init__(data_loader)
        self.country_code = country_code
        country_rics = self.data_loader.firm_lists.get_county_firm_rics_without_dead_firms(
            country=self.country_code,
            dead_date=min(interval_daily_returns),
            use_dead_list=use_dead_list,
        )
        for i, ric in enumerate(country_rics):
            self.firms[ric] = self.data_loader.get_firm(
                country_code=self.country_code,
                RIC=ric,
                interval_daily_returns=interval_daily_returns,
                interval_esg=interval_esg,
            )
