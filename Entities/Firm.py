import pandas as pd


class Firm:
    def __init__(
        self,
        meta: pd.Series,
        fundamentals: pd.DataFrame,
        risk_free_rate: pd.Series,
        market_returns: pd.Series,
        df_daily_returns: pd.DataFrame | None,
        df_esg: pd.DataFrame | None,
    ):
        self.meta = meta
        self.dscd: str = meta["Type"]
        self.ric: str = meta["RIC"]
        self.industry = meta["LocalScheme"]
        self.specific_industry = meta["RbssSchemeName"]

        self.fundamentals = fundamentals
        self.daily_returns: pd.Series = df_daily_returns["total_return"]
        self.daily_cumulative_returns: pd.Series = df_daily_returns["return_cumulative"]
        self.df_esg = df_esg

        risk_free_rate.name = "risk_free_returns"
        market_returns.name = "market_returns"
        self.capm = pd.concat([self.daily_returns, risk_free_rate, market_returns], axis=1, join="inner").dropna(axis=1, how="any")
        self.capm = self.capm.loc[~self.capm.eq(0).all(axis=1), :]
        self.capm.loc[:, "stock_premiums"] = self.capm["total_return"] - self.capm["risk_free_returns"]
        self.capm.loc[:, "market_premiums"] = self.capm["market_returns"] - self.capm["risk_free_returns"]

        self.mean_return: float = self.daily_returns.mean()
        self.median_return: float = self.daily_returns.median()
        self.vol_return: float = self.daily_returns.std()
        self.var_return: float = self.daily_returns.var()
        self.geometric_mean_return: float = self.daily_cumulative_returns.iloc[-1] ** (1 / len(self.daily_cumulative_returns))

        cov = self.capm["stock_premiums"].cov(self.capm["market_premiums"])
        var = self.capm["market_premiums"].var()
        self.beta: float = cov / var

        self.capm_returns: pd.Series = self.capm["stock_premiums"] - self.beta * self.capm["market_premiums"]

        self._smb = 0.0  # small minus big  small cap vs large cap
        self._smb_exposure = 0.0
        self._hms = 0.0  # high minus low book_values/market_values
        self._hms_exposure = 0.0
        self._rmw = 0.0  # robust vs weak profitability
        self._rmw_exposure = 0.0
        self._cma = 0.0  # conservative vs aggressive
        self._cma_exposure = 0.0

    @property
    def f3_returns(self):
        return self.capm_returns - self._smb_exposure * self._smb - self._hms_exposure * self._hms

    @property
    def f5_returns(self):
        return self.f3_returns - self._rmw_exposure * self._rmw - self._cma_exposure * self._cma
