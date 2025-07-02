from datetime import datetime

from Entities.FirmSelection import FirmSelection
from data_managemant.CountryCodes import COUNTRY
from data_managemant.DataLoader import DataLoader


class Country(FirmSelection):
    def __init__(
        self,
        data_loader: DataLoader,
        country_code: COUNTRY,
        interval_daily_returns: tuple[datetime, datetime] | None,
        interval_esg: tuple[int, int] | None,
        min_num_days: int | float = None,
        use_dead_list: bool = False,
        print_stuff: bool = True,
    ):
        self.country_code = country_code
        self.print_stuff = print_stuff
        country_rics = data_loader.firm_lists.get_county_firm_rics_without_dead_firms(
            country=self.country_code,
            dead_date=min(interval_daily_returns),
            use_dead_list=use_dead_list,
        )
        firms = {}
        for i, ric in enumerate(country_rics):
            if print_stuff:
                print(
                    f"{country_code.value+":":<4} {ric:<20} Load {i+1:<4.0f}/{len(country_rics):<4.0f} [{(i+1)/len(country_rics)*100:>6.2f}%]",
                )
            firms[ric] = data_loader.get_firm(
                country_code=self.country_code,
                RIC=ric,
                interval_daily_returns=interval_daily_returns,
                interval_esg=interval_esg,
                min_num_days=min_num_days,
            )
        super().__init__(firms=firms, name=country_code.value)
