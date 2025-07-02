import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from scipy.integrate import simpson
from scipy.stats import gaussian_kde

from Entities.Country import Country
from Entities.Firm import Firm
from Entities.FirmSelection import FirmSelection
from data_managemant.CountryCodes import COUNTRY
from data_managemant.DataLoader import DataLoader
from data_managemant.FileManager import FileManager


class BTTUM:
    FOLDER_DATA: str = os.path.dirname(os.path.realpath(__file__)) + r"\..\data"
    OUTPUT_RESULT_FOLDER: str = os.path.join(FOLDER_DATA, "results")

    def __init__(
        self,
        country_codes: list[COUNTRY],
        interval_daily_returns: tuple[datetime, datetime] = (datetime(2010, 1, 1), datetime(2025, 1, 1)),
        interval_esg: tuple[int, int] = (2005, 2030),
        use_dead_list: bool = True,
        min_num_firms: int = 5,
        print_loading=True,
    ):
        self.country_codes: list[COUNTRY] = country_codes
        self.interval_daily_returns: tuple[datetime, datetime] = interval_daily_returns
        self.interval_esg: tuple[int, int] = interval_esg
        self.use_dead_list: bool = use_dead_list

        self.data_loader = DataLoader(print_stuff=print_loading)
        self.all_firms: list[Firm] = []

        self.countries: dict[COUNTRY, Country] = {}
        self.country_return_tests: dict[COUNTRY, dict[str, pd.DataFrame]] = {}
        self.country_esg_tests: dict[str, pd.DataFrame] = {}

        for country_code in self.country_codes:
            self.countries[country_code] = Country(
                data_loader=self.data_loader,
                country_code=country_code,
                interval_daily_returns=self.interval_daily_returns,
                interval_esg=self.interval_esg,
                use_dead_list=self.use_dead_list,
                min_num_days=0.1,
            )
            self.all_firms.extend(self.countries[country_code].firms.values())
        self.broad_industries: dict[str, FirmSelection] = {}
        self.broad_industry_return_tests: dict[str, dict[str, pd.DataFrame]] = {}
        self.broad_industry_esg_tests: dict[str, pd.DataFrame] = {}
        self.all_broad_industries = sorted({firm.broad_industry for firm in self.all_firms if not pd.isna(firm.industry)})
        for broad_industry in self.all_broad_industries:
            firms = {firm.ric: firm for firm in self.all_firms if firm.broad_industry == broad_industry}
            if min_num_firms <= len(firms):
                print(f"CALC {broad_industry} with {len(firms)} firms")
                self.broad_industries[broad_industry] = FirmSelection(firms=firms, name=broad_industry)
            else:
                print(f"SKIP {broad_industry} since length is only {len(firms)}")
        print()

    def execute(
        self,
        check_ret_dates: list[datetime] | tuple[datetime, datetime],
        check_esg_years: list[int] | tuple[int, int],
        plot_esg: bool = False,
    ) -> None:
        print("Execute")
        if isinstance(check_ret_dates, tuple):
            if len(check_ret_dates) == 1:
                check_ret_dates = list[check_ret_dates]
            elif len(check_ret_dates) == 2:
                check_ret_dates = pd.date_range(check_ret_dates[0], check_ret_dates[1]).to_list()
            else:
                raise ValueError("check_dates must be a tuple of length 1 or 2")
        elif not isinstance(check_ret_dates, list):
            raise ValueError("check_dates must be a tuple or list")
        if isinstance(check_esg_years, tuple):
            if len(check_esg_years) == 1:
                check_esg_years = list[check_esg_years]
            elif len(check_esg_years) == 2:
                check_esg_years = list(range(min(check_esg_years), max(check_esg_years) + 1))
            else:
                raise ValueError("check_dates must be a tuple of length 1 or 2")
        elif not isinstance(check_esg_years, list):
            raise ValueError("check_dates must be a tuple or list")

        self.plot_return_distribution()

        master_country_returns = []
        for country_code, country in self.countries.items():
            country_return_test = country.test_returns_at_dates_summary(
                dates=check_ret_dates,
                excel_name=f"{country_code.value}\\"
                f"TEST_RETURN__"
                f"__{min(self.interval_daily_returns).strftime('%Y-%m-%d')}"
                f"_{max(self.interval_daily_returns).strftime('%Y-%m-%d')}"
                f"__{min(check_ret_dates).strftime('%Y-%m-%d')}"
                f"_{max(check_ret_dates).strftime('%Y-%m-%d')}",
            )
            self.country_return_tests[country_code] = country_return_test
            df = country_return_test["master_comp"].copy()
            if not df.empty:
                df.reset_index(inplace=True, drop=False)
                df.loc[:, "country"] = country_code.value
                df.set_index(["country", "date", "return_type", "z_score"], inplace=True, drop=True)
                df.sort_index(inplace=True)
                master_country_returns.append(df)

            self.country_esg_tests[country_code.value] = country.test_esg(
                years=check_esg_years,
                excel_name=f"{country_code.value}\\" f"ESG__" f"__{min(check_esg_years)}" f"__{max(check_esg_years)}",
            )
        master_country_return = pd.concat(master_country_returns, axis="rows")
        FileManager.write_excel_results(
            f"{'_'.join([cc.value for cc in self.country_codes])}\\"
            f"TEST_RETURN"
            f"__MASTER_COMP_COUNTRIES"
            f"__{min(self.interval_daily_returns).strftime('%Y-%m-%d')}"
            f"_{max(self.interval_daily_returns).strftime('%Y-%m-%d')}"
            f"__{min(check_ret_dates).strftime('%Y-%m-%d')}"
            f"_{max(check_ret_dates).strftime('%Y-%m-%d')}",
            {"master_comp": master_country_return},
        )

        master_broad_industry_returns = []
        for broad_industry_name, broad_industry in self.broad_industries.items():
            broad_industry_return_test = broad_industry.test_returns_at_dates_summary(
                dates=check_ret_dates,
                excel_name=f"{'_'.join([cc.value for cc in self.country_codes])}\\"
                f"TEST_RETURN"
                f"__{broad_industry_name.upper().replace(' ', '_').replace("|","_")}"
                f"__{min(self.interval_daily_returns).strftime('%Y-%m-%d')}"
                f"_{max(self.interval_daily_returns).strftime('%Y-%m-%d')}"
                f"__{min(check_ret_dates).strftime('%Y-%m-%d')}"
                f"_{max(check_ret_dates).strftime('%Y-%m-%d')}",
            )
            self.broad_industry_return_tests[broad_industry_name] = broad_industry_return_test
            df = broad_industry_return_test["master_comp"].copy()
            if not df.empty:
                df.reset_index(inplace=True, drop=False)
                df.loc[:, "broad_industry"] = broad_industry_name
                df.set_index(["broad_industry", "date", "return_type", "z_score"], inplace=True, drop=True)
                df.sort_index(inplace=True)
                master_broad_industry_returns.append(df)

            self.broad_industry_esg_tests[broad_industry_name] = broad_industry.test_esg(
                years=check_esg_years,
                excel_name=f"{'_'.join([cc.value for cc in self.country_codes])}\\"
                f"ESG"
                f"__{broad_industry_name.upper().replace(' ', '_').replace("|","_")}"
                f"__{min(check_esg_years)}"
                f"_{max(check_esg_years)}",
            )

        master_comp_broad_industry_return = pd.concat(master_broad_industry_returns, axis="rows").copy()
        FileManager.write_excel_results(
            f"{'_'.join([cc.value for cc in self.country_codes])}\\"
            f"TEST_RETURN"
            f"__MASTER_COMP_BROAD_INDUSTRIES"
            f"__{min(self.interval_daily_returns).strftime('%Y-%m-%d')}"
            f"_{max(self.interval_daily_returns).strftime('%Y-%m-%d')}"
            f"__{min(check_ret_dates).strftime('%Y-%m-%d')}"
            f"_{max(check_ret_dates).strftime('%Y-%m-%d')}",
            {"master_comp": master_comp_broad_industry_return},
        )

        if plot_esg:
            self.plot_esg(check_ret_dates=check_esg_years)

    def plot_return_distribution(self):
        # Assuming self.all_firms is already defined and each has .daily_returns
        returns = np.concatenate(
            [firm.daily_returns for firm in self.all_firms if isinstance(firm.daily_returns, pd.Series) and not firm.daily_returns.empty],
            axis=0,
        )
        # Remove NaNs
        returns = returns[~np.isnan(returns)]
        returns = returns[returns != 0]

        # KDE
        kde = gaussian_kde(returns, bw_method=1.0)
        x = np.linspace(-3, 3, 2000)
        y = kde(x)
        print("Return Distribution: area under the KDE curve:", simpson(y, x))

        # Create plotly figure
        scale_factor = 2
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                mode="lines",
                name="KDE",
                line=dict(width=3.5 * scale_factor),
            )
        )
        fig.update_layout(
            xaxis_title="Return",
            yaxis_title="Density",
            template="simple_white",  # clean look
            xaxis=dict(
                showgrid=False,
                gridwidth=0.5 * scale_factor,
                gridcolor="lightgray",
                # range=[-2, 2],
                title_font=dict(size=33 * scale_factor),
                tickfont=dict(size=30 * scale_factor),
                zeroline=True,
                zerolinewidth=1 * scale_factor,
                zerolinecolor="grey",
            ),
            yaxis=dict(
                showgrid=True,
                gridwidth=0.5 * scale_factor,
                gridcolor="lightgray",
                title_font=dict(size=33 * scale_factor),
                tickfont=dict(size=30 * scale_factor),
            ),
        )

        print("Plot Return Distribution")
        FileManager.save_fig(
            fig=fig,
            name=f"{'_'.join([cc.value for cc in self.country_codes])}\\"
            f"RETURN_DISTRIBUTION"
            f"__{min(self.interval_daily_returns).strftime('%Y-%m-%d')}"
            f"__{max(self.interval_daily_returns).strftime('%Y-%m-%d')}",
        )

    def plot_esg(self, check_ret_dates: list[int]):
        print("Plot Country ESG")
        self.plot_esg_dfs(
            dfs=self.country_esg_tests,
            column="esg_score",
            name=f"{'_'.join(self.country_esg_tests.keys())}\\"
            f"PLOT_COUNTRY_ESG"
            f"__{min(self.interval_esg)}"
            f"_{max(self.interval_esg)}"
            f"__{min(check_ret_dates)}"
            f"_{max(check_ret_dates)}",
            yaxis_title="ESG Score",
            legend_title="Countries:",
        )
        print("Plot Country Environmental")
        self.plot_esg_dfs(
            dfs=self.country_esg_tests,
            column="environmental_pillar_score",
            name=f"{'_'.join(self.country_esg_tests.keys())}\\"
            f"PLOT_COUNTRY_ENVIRONMENT"
            f"__{min(self.interval_esg)}"
            f"_{max(self.interval_esg)}"
            f"__{min(check_ret_dates)}"
            f"_{max(check_ret_dates)}",
            yaxis_title="Environmental Pillar Score",
            legend_title="Countries:",
        )
        print("Plot Broad Industries ESG")
        self.plot_esg_dfs(
            dfs=self.broad_industry_esg_tests,
            column="esg_score",
            name=f"{'_'.join(self.country_esg_tests.keys())}\\"
            f"PLOT_BROAD_INDUSTRIES_ESG"
            f"__{min(self.interval_esg)}"
            f"_{max(self.interval_esg)}"
            f"__{min(check_ret_dates)}"
            f"_{max(check_ret_dates)}",
            yaxis_title="ESG Score",
            legend_title="Industries:",
        )
        print("Plot Broad Industries Environmental")
        self.plot_esg_dfs(
            dfs=self.broad_industry_esg_tests,
            column="environmental_pillar_score",
            name=f"{'_'.join(self.country_esg_tests.keys())}\\"
            f"PLOT_BROAD_INDUSTRIES_ENVIRONMENT"
            f"__{min(self.interval_esg)}"
            f"_{max(self.interval_esg)}"
            f"__{min(check_ret_dates)}"
            f"_{max(check_ret_dates)}",
            yaxis_title="Environmental Pillar Score",
            legend_title="Industries:",
        )

    @staticmethod
    def plot_esg_dfs(
        dfs: dict[str, pd.DataFrame],
        column: str,
        name: str | None = None,
        show: bool = True,
        yaxis_title: str = "Score",
        legend_title="Legend",
    ) -> None:
        dfs = [df.set_index("date").loc[:, column].rename(name) for name, df in dfs.items() if not df.empty]
        if len(dfs) <= 0:
            return

        df = (
            pd.concat(
                dfs,
                axis="columns",
                join="outer",
            )
            .sort_index()
            .rename(index=lambda x: x.strftime("%Y-%m-%d"))
        )
        if name is not None:
            FileManager.save_fig(
                fig=BTTUM._plot_esg_df(
                    df=df,
                    yaxis_title=yaxis_title,
                    legend_title=legend_title,
                    scale_factor=2.5,
                ),
                name=name,
            )
        if show:
            BTTUM._plot_esg_df(
                df=df,
                yaxis_title=yaxis_title,
                legend_title=legend_title,
                scale_factor=0.25,
            ).show()

    @staticmethod
    def _smart_linebreak(s, n=25):
        words = s.split()
        lines = []
        current_line = ""
        for word in words:
            if len(current_line) + len(word) + (1 if current_line else 0) <= n:
                current_line += (" " if current_line else "") + word
            else:
                if current_line:
                    lines.append(current_line)
                # If the word itself is longer than n, split it
                while len(word) > n:
                    lines.append(word[:n])
                    word = word[n:]
                current_line = word
        if current_line:
            lines.append(current_line)
        return "<br>".join(lines)

    @staticmethod
    def _plot_esg_df(
        df: pd.DataFrame,
        yaxis_title: str = "Score",
        legend_title: str = "Legend",
        scale_factor: float = 2.5,
    ) -> go.Figure:
        colors = (
            ["blue"] if len(df.columns) <= 1 else px.colors.sample_colorscale("Rainbow", [n / (len(df.columns) - 1) for n in range(len(df.columns))])
        )
        fig = go.Figure()
        for column, color in zip(df.columns, colors):
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df[column],
                    mode="lines+markers",
                    name=BTTUM._smart_linebreak(column),
                    line=dict(
                        width=3.5 * scale_factor,
                        color=color,
                    ),
                    marker=dict(size=10 * scale_factor),
                )
            )

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title=yaxis_title,
            yaxis=dict(
                range=[-5, 105],
                showgrid=True,
                gridcolor="lightgrey",
                gridwidth=0.5 * scale_factor,
                tickvals=list(range(0, 110, 10)),
                zeroline=True,
                zerolinewidth=1,
                zerolinecolor="lightgrey",
                title_font=dict(size=33 * scale_factor),
                tickfont=dict(size=30 * scale_factor),
            ),
            xaxis=dict(
                showgrid=False,
                title_font=dict(size=33 * scale_factor),
                tickfont=dict(size=30 * scale_factor),
            ),
            legend=dict(
                itemsizing="trace",  # try 'trace','constant' to see the difference
                title_text=legend_title,
                font=dict(
                    size=22 * scale_factor,
                ),
            ),
            font=dict(size=30 * scale_factor),
            margin=dict(t=0, b=50, l=50, r=200),
            plot_bgcolor="white",
            paper_bgcolor="white",
        )

        return fig
