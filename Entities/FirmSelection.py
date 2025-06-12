import pandas as pd

from Entities.Firm import Firm


class FirmSelection:

    def __init__(self, dataloader):
        self.data_loader = dataloader
        self.firms: dict[str, Firm] = {}
        self._avg_return = None

    def get_attribute(self, func) -> pd.Series:
        return pd.Series(self.firms.values()).map(func)

    @property
    def mean_mean_return(self) -> float:
        if self._avg_return is None:
            self._avg_return = self.get_attribute(lambda firm: firm.mean_return).mean()
        return self._avg_return

    @property
    def median_mean_return(self) -> float:
        return self.get_attribute(lambda firm: firm.mean_return).median()

    @property
    def mean_median_return(self):
        return self.get_attribute(lambda firm: firm.median_return).mean()

    @property
    def median_median_return(self):
        return self.get_attribute(lambda firm: firm.median_return).median()

    @property
    def mean_vol(self) -> float:
        return self.get_attribute(lambda firm: firm.vol_return).mean()

    @property
    def median_vol(self) -> float:
        return self.get_attribute(lambda firm: firm.vol_return).median()

    def returns_at(self):

        return
