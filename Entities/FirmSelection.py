import copy
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.integrate import simpson
from scipy.stats import gaussian_kde, norm


from Entities.Firm import Firm
from data_managemant.FileManager import FileManager


class FirmSelection:

    def __init__(self, firms: dict[str, Firm], name: str = None) -> None:
        self.firms: dict[str, Firm] = copy.deepcopy(firms)
        print(f"\t{name} firms                          {' '*(50-len(name))} {len(self.firms):>4.0f}")
        self.firms_with_fundamentals: dict[str, Firm] = {}
        for ric, firm in self.firms.items():
            if firm.fundamentals is not None and firm.daily_returns is not None:
                self.firms_with_fundamentals[ric] = firm
        print(
            f"\t{name} firms with fundamentals:{' '*(50-len(name))} {len(self.firms_with_fundamentals):>4.0f} / {len(self.firms):>4.0f} [{len(self.firms_with_fundamentals)/len(self.firms):>7.2%}]"
        )
        self.firms_with_esg: dict[str, Firm] = {}
        for ric, firm in self.firms.items():
            if isinstance(firm.df_esg, pd.DataFrame) and not firm.df_esg.empty:
                self.firms_with_esg[ric] = firm
        print(
            f"\t{name} firms with esg:         {' '*(50-len(name))} {len(self.firms_with_esg):>4.0f} / {len(self.firms):>4.0f} [{len(self.firms_with_esg)/len(self.firms):>7.2%}]"
        )
        self.set_factors()

    @property
    def returns(self) -> np.ndarray:
        return np.concatenate([firm.daily_returns for firm in self.firms_with_fundamentals.values()], axis=0)

    def set_factors(self):
        smb_cut = np.quantile([firm.smb_categorizer for firm in self.firms_with_fundamentals.values()], 0.5)

        smb_low_mean = (
            pd.concat(
                [firm.daily_returns.rename(firm.ric) for firm in self.firms_with_fundamentals.values() if firm.smb_categorizer < smb_cut],
                axis=1,
                join="outer",
            )
            .mean(axis=1)
            .rename("smb_low_mean")
        )
        smb_high_mean = (
            pd.concat(
                [firm.daily_returns.rename(firm.ric) for firm in self.firms_with_fundamentals.values() if smb_cut <= firm.smb_categorizer],
                axis=1,
                join="outer",
            )
            .mean(axis=1)
            .rename("smb_high_mean")
        )
        smb = (smb_low_mean - smb_high_mean).dropna()

        low_hms_cut, high_hms_cut = np.quantile([firm.hms_categorizer for firm in self.firms_with_fundamentals.values()], q=[0.3, 0.7])
        hms_low_mean = (
            pd.concat(
                [firm.daily_returns.rename(firm.ric) for firm in self.firms_with_fundamentals.values() if firm.hms_categorizer <= low_hms_cut],
                axis=1,
                join="outer",
            )
            .mean(axis=1)
            .rename("hms_low_mean")
        )
        hms_high_mean = (
            pd.concat(
                [firm.daily_returns.rename(firm.ric) for firm in self.firms_with_fundamentals.values() if high_hms_cut <= firm.hms_categorizer],
                axis=1,
                join="outer",
            )
            .mean(axis=1)
            .rename("hms_high_mean")
        )
        hms = (hms_low_mean - hms_high_mean).dropna()

        low_rmw_cut, high_rmw_cut = np.quantile([firm.rmw_categorizer for firm in self.firms_with_fundamentals.values()], q=[0.3, 0.7])
        rmw_low_mean = (
            pd.concat(
                [firm.daily_returns.rename(firm.ric) for firm in self.firms_with_fundamentals.values() if firm.rmw_categorizer <= low_rmw_cut],
                axis=1,
                join="outer",
            )
            .mean(axis=1)
            .rename("low_rmw_cut")
        )
        rmw_high_mean = (
            pd.concat(
                [firm.daily_returns.rename(firm.ric) for firm in self.firms_with_fundamentals.values() if high_rmw_cut <= firm.rmw_categorizer],
                axis=1,
                join="outer",
            )
            .mean(axis=1)
            .rename("high_rmw_cut")
        )
        rmw = (rmw_low_mean - rmw_high_mean).dropna()

        low_cma_cut, high_cma_cut = np.quantile([firm.cma_categorizer for firm in self.firms_with_fundamentals.values()], q=[0.3, 0.7])
        cma_low_mean = (
            pd.concat(
                [firm.daily_returns.rename(firm.ric) for firm in self.firms_with_fundamentals.values() if firm.cma_categorizer <= low_cma_cut],
                axis=1,
                join="outer",
            )
            .mean(axis=1)
            .rename("low_cma_cut")
        )
        cma_high_mean = (
            pd.concat(
                [firm.daily_returns.rename(firm.ric) for firm in self.firms_with_fundamentals.values() if high_cma_cut <= firm.cma_categorizer],
                axis=1,
                join="outer",
            )
            .mean(axis=1)
            .rename("high_cma_cut")
        )
        cma = (cma_low_mean - cma_high_mean).dropna()

        for firm in self.firms.values():
            firm.set_factors(
                smb=smb,
                hms=hms,
                rmw=rmw,
                cma=cma,
            )

    def plot_return_distribution(self, title: str = None):
        kde = gaussian_kde(self.returns[~np.isnan(self.returns)], bw_method=1.0)  # Adjust bw_method if needed
        x = np.linspace(min(self.returns), max(self.returns), 1000)
        y = kde(x)
        # Calculate area under curve (should be 1)
        area = simpson(y, x)
        print("Area under the KDE curve:", area)
        plt.figure(figsize=(8, 5))
        plt.plot(x, y)
        if title is None:
            plt.title("Continuous Probability Density of Returns")
        else:
            plt.title(title)
        plt.xlabel("Return")
        plt.ylabel("Density")
        plt.grid(True, linestyle="--", alpha=0.5)
        plt.show()

    def test_returns_at_dates(self, dates: list[datetime]) -> dict[str, pd.DataFrame]:
        results = {
            "_ret_zscores": pd.concat(
                [firm.ret_zscore(dates=dates).rename(firm.ric) for firm in self.firms_with_fundamentals.values()],
                axis=1,
                join="outer",
            ),
            "capm_zscores": pd.concat(
                [firm.capm_zscore(dates=dates).rename(firm.ric) for firm in self.firms_with_fundamentals.values()],
                axis=1,
                join="outer",
            ),
            "f3_zscores": pd.concat(
                [firm.f3_zscore(dates=dates).rename(firm.ric) for firm in self.firms_with_fundamentals.values()],
                axis=1,
                join="outer",
            ),
            "f5_zscores": pd.concat(
                [firm.f5_zscore(dates=dates).rename(firm.ric) for firm in self.firms_with_fundamentals.values()],
                axis=1,
                join="outer",
            ),
        }
        return results

    def test_returns_at_dates_summary(
        self,
        dates: list[datetime],
        z_score_limits: list[float] = None,
        print_stats: bool = False,
        excel_name: str = None,
    ) -> dict[str, pd.DataFrame]:
        if z_score_limits is None:
            z_score_limits = [1.645, 1.96, 2.575, 3.0]
        z_score_limits_perc = {z: (norm.cdf(-z) - norm.cdf(z) + 1.0) for z in z_score_limits}

        test_results = self.test_returns_at_dates(dates=dates)
        breach_dfs = {}
        comp_dfs = {}
        for return_type, z_scores in test_results.items():
            # real
            df_real_breaches = {lim: z_scores.abs().gt(lim) for lim in z_score_limits}
            df_real = pd.DataFrame({lim: z_scores.abs().gt(lim).sum(axis=1) for lim in z_score_limits})
            # df = df.rename(columns=lambda z: str(z))
            not_nan_count = z_scores.count(axis=1)
            df_real.insert(0, "Firms_with_return", not_nan_count)
            df_real = pd.concat([df_real, df_real.sum(axis=0).rename("Total").to_frame().T], axis=0)
            df_real.index.name = return_type
            breach_dfs[f"{return_type}_real"] = df_real

            # exp
            df_exp: pd.DataFrame = df_real.copy().loc[df_real.index != "Total", :]
            for col in z_score_limits:
                df_exp[col] = np.ceil(df_exp.loc[:, "Firms_with_return"] * z_score_limits_perc[col]).astype("float")
            breach_dfs[f"{return_type}_exp"] = df_exp

            real_ser = (
                df_real.loc[df_real.index != "Total", z_score_limits]
                .reset_index(names="date", drop=False)
                .melt(
                    id_vars=["date"],
                    value_vars=z_score_limits,
                    var_name="z_score",
                    value_name="real_amount",
                )
                .set_index(["date", "z_score"])
            )

            exp_ser = (
                df_exp.reset_index(names="date", drop=False)
                .melt(
                    id_vars=["date"],
                    value_vars=z_score_limits,
                    var_name="z_score",
                    value_name="exp_amount",
                )
                .set_index(["date", "z_score"])
            )
            comp = pd.merge(left=exp_ser, right=real_ser, left_index=True, right_index=True, how="outer")
            comp.loc[:, "exp_gt_real"] = comp.loc[:, "real_amount"] > comp.loc[:, "exp_amount"]
            comp = comp.loc[comp.loc[:, "exp_gt_real"], :]
            if not comp.empty:
                comp.loc[:, "firms"] = None
                for i, row in comp.iterrows():
                    ser = df_real_breaches[i[1]].loc[i[0], :].copy()
                    comp.at[i, "firms"] = f'[{" | ".join(sorted(ser[ser].index))}]'

            breach_dfs[f"{return_type}_comp"] = comp.copy()

            if not comp.empty:
                comp = comp.reset_index(drop=False)
                comp.loc[:, "return_type"] = return_type
                comp = comp.set_index(["date", "return_type", "z_score"], drop=True)

            comp_dfs[f"{return_type}_comp"] = comp.copy()

            if print_stats:
                print(df_real)
                print(df_exp)
                print(comp)

        master_dfs = [df for df in comp_dfs.values() if not df.empty]
        master_df = pd.DataFrame() if len(master_dfs) <= 0 else pd.concat(master_dfs, axis="rows").sort_index()
        breach_dfs = {"master_comp": master_df} | breach_dfs

        if excel_name is not None:
            FileManager.write_excel_results(excel_name=excel_name, dfs=breach_dfs)
        return breach_dfs

    def test_esg(self, years: list[int] | None, excel_name: None | str = None) -> pd.DataFrame:
        con_esg: pd.DataFrame = pd.concat(
            [firm.df_esg for firm in self.firms_with_esg.values()],
            axis="rows",
        )
        mean_esg = con_esg.groupby(by="date").mean()

        if years is not None:
            year_end_dates = [datetime(year, 12, 31) for year in years]
            mean_esg = pd.merge(
                left=pd.Series(data=year_end_dates, name="date"),
                right=mean_esg,
                how="left",
                on="date",
            )

        if excel_name is not None:
            FileManager.write_excel_results(excel_name, {"MEAN_ESG_VALUES": mean_esg})
        return mean_esg
