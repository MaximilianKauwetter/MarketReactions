from datetime import datetime

import numpy as np
import pandas as pd
import statsmodels.api as sm


industry_mapper = {
    "Basic Materials Industry and Construction": "Basic Industry",
    "Basic Metals and Chemicals": "Basic Industry",
    "Basic Industries": "Basic Industry",
    "Finance, Insurance and Real Estate": "Financials",
    "Financial Services and Real Estate": "Financials",
}


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
        # meta attributes
        self.meta = meta
        self.dscd: str = meta["Type"]
        self.ric: str = meta["RIC"]
        self.industry: str = meta["LocalScheme"]
        self.broad_industry = self.industry if pd.isna(self.industry) else self.industry.split("/")[0]
        if pd.notna(self.broad_industry):
            if self.broad_industry.startswith("Other Services|"):
                self.broad_industry = self.broad_industry.split("|")[1]
            self.broad_industry = self.broad_industry.split("|")[0]
            if self.broad_industry in industry_mapper.keys():
                self.broad_industry = industry_mapper[self.broad_industry]

        self.specific_industry = meta["RbssSchemeName"]

        # fundamentals
        self.fundamentals = fundamentals
        self.market_capitalization = None if fundamentals is None else self.fundamentals["market_cap"].dropna()
        self.book_equity = None if fundamentals is None else self.fundamentals["book_equity"].dropna()
        self.op_profit = None if fundamentals is None else self.fundamentals["ebit"].dropna()
        self.int_exp = None if fundamentals is None else self.fundamentals["int_exp"].dropna()
        self.assets = None if fundamentals is None else self.fundamentals["tot_assets"].dropna()

        # esg
        self.df_esg = df_esg

        if df_daily_returns is None:
            self.daily_returns = None
        else:
            # returns
            risk_free_rate.name = "risk_free_returns"
            market_returns.name = "market_returns"
            returns = pd.concat([df_daily_returns["total_return"], risk_free_rate, market_returns], axis=1, join="inner").dropna(axis=0, how="any")
            returns = returns.loc[~returns.eq(0).all(axis=1), :]
            self.daily_returns: pd.Series = returns["total_return"]
            self.stock_premiums: pd.Series = returns["total_return"] - returns["risk_free_returns"]
            self.stock_premiums.name = "SP"
            self.market_premiums = returns["market_returns"] - returns["risk_free_returns"]
            self.market_premiums.name = "MP"

            self.mean_return: float = self.daily_returns.mean()
            self.median_return: float = self.daily_returns.median()
            self.vol_return: float = self.daily_returns.std()
            self.var_return: float = self.daily_returns.var()

            # geometric mean return
            if 0 < len(df_daily_returns):
                self.geometric_mean_return: float = df_daily_returns["return_cumulative"].iloc[-1] ** (1 / len(df_daily_returns["return_cumulative"]))
            else:
                self.geometric_mean_return: float = 0

            # CAPM
            cov = self.stock_premiums.cov(self.market_premiums)
            var = self.market_premiums.var()
            self.beta: float = cov / var
            self.capm_returns: pd.Series = self.stock_premiums - (self.beta * self.market_premiums)

            # factor categorizer
            self.smb_categorizer = None if fundamentals is None else self.market_capitalization.mean()
            self.hms_categorizer = None if fundamentals is None else (self.book_equity / self.market_capitalization).mean()
            self.rmw_categorizer = None if fundamentals is None else ((self.op_profit - self.int_exp) / self.book_equity).mean()
            self.cma_categorizer = None if fundamentals is None else self.assets.pct_change().dropna().mean()

            # general factor values
            self._smb = np.zeros(len(self.stock_premiums))  # small minus big  small cap vs large cap
            self._hms = np.zeros(len(self.stock_premiums))  # high minus low book_values/market_values
            self._rmw = np.zeros(len(self.stock_premiums))  # robust vs weak profitability
            self._cma = np.zeros(len(self.stock_premiums))  # conservative vs aggressive

            # company factor exposures
            self._alpha3 = 0.0
            self._beta3_exposure = 0.0
            self._smb3_exposure = 0.0
            self._hms3_exposure = 0.0
            self._alpha5 = 0.0
            self._beta5_exposure = 0.0
            self._smb5_exposure = 0.0
            self._hms5_exposure = 0.0
            self._rmw5_exposure = 0.0
            self._cma5_exposure = 0.0

    @property
    def f3_returns(self):
        if self.fundamentals is None or self.daily_returns is None:
            return None
        f3_explained = self._alpha3 + self._beta3_exposure * self.market_premiums + self._smb3_exposure * self._smb + self._hms3_exposure * self._hms
        return self.stock_premiums - f3_explained

    @property
    def f5_returns(self):
        if self.fundamentals is None or self.daily_returns is None:
            return None
        f5_explained = (
            self._alpha5
            + self._beta5_exposure * self.market_premiums
            + self._smb5_exposure * self._smb
            + self._hms5_exposure * self._hms
            + self._rmw5_exposure * self._rmw
            + self._cma5_exposure * self._cma
        )

        return self.stock_premiums - f5_explained

    def set_factors(
        self,
        smb: pd.Series,
        hms: pd.Series,
        rmw: pd.Series,
        cma: pd.Series,
    ):
        if self.fundamentals is None or self.daily_returns is None:
            return
        self._smb = smb
        self._smb.name = "SMB"
        self._hms = hms
        self._hms.name = "HMS"
        self._rmw = rmw
        self._rmw.name = "RMW"
        self._cma = cma
        self._cma.name = "CMA"

        ols = pd.concat(
            [
                self.stock_premiums.copy(),
                self.market_premiums.copy(),
                self._smb.copy(),
                self._hms.copy(),
                self._rmw.copy(),
                self._cma.copy(),
            ],
            axis=1,
            join="inner",
        ).astype(float)
        res3 = sm.OLS(
            ols["SP"],
            sm.add_constant(ols[["MP", "SMB", "HMS"]].copy()),
        ).fit()
        self._alpha3 = res3.params["const"]
        self._beta3_exposure = res3.params["MP"]
        self._smb3_exposure = res3.params["SMB"]
        self._hms3_exposure = res3.params["HMS"]

        res5 = sm.OLS(
            ols["SP"],
            sm.add_constant(ols[["MP", "SMB", "HMS", "RMW", "CMA"]].copy()),
        ).fit()
        self._alpha5 = res5.params["const"]
        self._beta5_exposure = res5.params["MP"]
        self._smb5_exposure = res5.params["SMB"]
        self._hms5_exposure = res5.params["HMS"]
        self._rmw5_exposure = res5.params["RMW"]
        self._cma5_exposure = res5.params["CMA"]

    @staticmethod
    def _zscore(returns: pd.Series, dates: list[datetime]) -> pd.Series:
        dates = returns.index.intersection(dates)
        x = returns.loc[dates]
        mean = returns.mean()
        std = returns.std()
        return (x - mean) / std

    def ret_zscore(self, dates: list[datetime]) -> pd.Series:
        return self._zscore(returns=self.daily_returns, dates=dates)

    def capm_zscore(self, dates: list[datetime]) -> pd.Series:
        return self._zscore(returns=self.capm_returns, dates=dates)

    def f3_zscore(self, dates: list[datetime]) -> pd.Series:
        return self._zscore(returns=self.f3_returns, dates=dates)

    def f5_zscore(self, dates: list[datetime]) -> pd.Series:
        return self._zscore(returns=self.f5_returns, dates=dates)
