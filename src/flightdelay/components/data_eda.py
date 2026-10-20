import sys
from typing import List, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import polars as pl

from flightdelay.utils.common import Paths, partition_file, write_json
from flightdelay.utils.exception import CustomException
from flightdelay.utils.logger import logger


class DataEDA:
    def __init__(self, cfg: dict, paths: Paths):
        self.paths = paths
        self.out = paths.artifacts / "eda"
        self.out.mkdir(parents=True, exist_ok=True)

    def _plot(self, labels, values, title, xlabel, fname, kind="bar"):
        fig, ax = plt.subplots(figsize=(10, 4))
        (ax.bar if kind == "bar" else ax.plot)(labels, values)
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("Delay rate (arrival 15+ min late)")
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        fig.tight_layout()
        fig.savefig(self.out / fname, dpi=130)
        plt.close(fig)

    def run(self, months: List[Tuple[int, int]]) -> dict:
        try:
            files = [str(f) for y, m in months
                     if (f := partition_file(self.paths.processed, "flights", y, m)).exists()]
            if not files:
                raise FileNotFoundError("No processed data. Run transformation first.")
            lf = pl.scan_parquet(files, hive_partitioning=False)
            n, rate = pl.len().alias("n_flights"), pl.col("ArrDel15").mean().alias("delay_rate")

            q = {
                "overall": lf.select(n, rate),
                "by_hour": lf.group_by("dep_hour_local").agg(n, rate).sort("dep_hour_local"),
                "by_dow": lf.group_by("dep_dow_local").agg(n, rate).sort("dep_dow_local"),
                "by_month": lf.group_by(["Year", "Month"]).agg(n, rate).sort(["Year", "Month"]),
                "by_airline": lf.group_by("Reporting_Airline").agg(n, rate).sort("n_flights", descending=True),
                "top_origins": lf.group_by("Origin").agg(n, rate).sort("n_flights", descending=True).head(20),
            }
            res = dict(zip(q.keys(), pl.collect_all(list(q.values()))))
            for name, df in res.items():
                df.write_csv(self.out / f"{name}.csv")

            h, w, mth = res["by_hour"], res["by_dow"], res["by_month"]
            self._plot(h["dep_hour_local"].cast(pl.Utf8).to_list(), h["delay_rate"].to_list(),
                       "Delay rate by scheduled departure hour (local)", "Hour", "delay_by_hour.png")
            self._plot(w["dep_dow_local"].cast(pl.Utf8).to_list(), w["delay_rate"].to_list(),
                       "Delay rate by day of week (1=Mon)", "Day", "delay_by_dow.png")
            self._plot([f"{y}-{m:02d}" for y, m in zip(mth["Year"], mth["Month"])],
                       mth["delay_rate"].to_list(), "Monthly delay rate", "Month",
                       "delay_by_month.png", kind="line")
            a = res["by_airline"]
            self._plot(a["Reporting_Airline"].to_list(), a["delay_rate"].to_list(),
                       "Delay rate by airline", "Airline", "delay_by_airline.png")
            t = res["top_origins"]
            self._plot(t["Origin"].to_list(), t["delay_rate"].to_list(),
                       "Delay rate, 20 busiest origin airports", "Airport", "delay_by_origin.png")

            summary = res["overall"].row(0, named=True)
            write_json(summary, self.out / "summary.json")
            logger.info(f"EDA done: {summary['n_flights']:,} flights, delay rate {summary['delay_rate']:.3f}")
            return summary
        except Exception as e:
            raise CustomException(e, sys)