import sys
from typing import List, Tuple

import polars as pl

from flightdelay.constants import KEY_COLUMNS
from flightdelay.utils.common import Paths, partition_file, write_json
from flightdelay.utils.exception import CustomException
from flightdelay.utils.logger import logger


class DataValidation:
    def __init__(self, cfg: dict, paths: Paths):
        self.cfg = cfg
        self.v = cfg["validation"]
        self.paths = paths

    def run(self, months: List[Tuple[int, int]]) -> dict:
        files = [str(f) for y, m in months
                 if (f := partition_file(self.paths.interim, "flights", y, m)).exists()]
        if not files:
            raise CustomException(FileNotFoundError("No interim files. Run ingestion first."), sys)

        lf = pl.scan_parquet(files, hive_partitioning=False)
        airports = pl.read_parquet(self.paths.reference / "airports.parquet")["iata_code"].to_list()

        s = lf.select(
            pl.len().alias("n_rows"),
            *[pl.col(c).is_null().mean().alias(f"null__{c}") for c in self.v["max_null_rate"]],
            ((pl.col("CRSDepTime") < 0) | (pl.col("CRSDepTime") > 2400)).sum().alias("bad_crs_dep_time"),
            (pl.col("Distance") <= 0).sum().alias("bad_distance"),
            ((pl.col("CRSElapsedTime") < 1) | (pl.col("CRSElapsedTime") > 1500)).sum().alias("bad_elapsed"),
            ((pl.col("Cancelled") < 0) | (pl.col("Cancelled") > 1)).sum().alias("bad_cancelled_flag"),
            ((pl.col("ArrDelay") >= 15) != (pl.col("ArrDel15") == 1)).sum().alias("inconsistent_target"),
            (~pl.col("Origin").is_in(airports) | ~pl.col("Dest").is_in(airports)).sum().alias("unknown_airport"),
        ).collect().row(0, named=True)

        dups = (lf.group_by(KEY_COLUMNS).len().filter(pl.col("len") > 1)
                  .select((pl.col("len") - 1).sum().alias("d")).collect()["d"][0]) or 0
        by_month = lf.group_by(["Year", "Month"]).len().sort(["Year", "Month"]).collect()

        n = s["n_rows"]
        checks = []

        def check(name, value, limit, severity="error"):
            checks.append({"check": name, "value": float(value), "limit": float(limit),
                           "severity": severity, "passed": bool(value <= limit)})

        for col, limit in self.v["max_null_rate"].items():
            check(f"null_rate[{col}]", s[f"null__{col}"], limit)
        check("duplicate_rate", dups / n, self.v["max_duplicate_rate"])
        check("unknown_airport_rate", s["unknown_airport"] / n, self.v["max_unknown_airport_rate"])
        for k in ("bad_crs_dep_time", "bad_distance", "bad_elapsed", "bad_cancelled_flag"):
            check(f"{k}_rate", s[k] / n, self.v["max_bad_value_rate"])
        check("inconsistent_target_rate", s["inconsistent_target"] / n,
              self.v["max_inconsistent_target_rate"])
        for y, m, cnt in by_month.iter_rows():
            checks.append({"check": f"min_rows[{y}-{m:02d}]", "value": cnt,
                           "limit": self.v["min_rows_per_month"], "severity": "error",
                           "passed": cnt >= self.v["min_rows_per_month"]})

        report = {"n_rows": n, "n_files": len(files), "checks": checks}
        write_json(report, self.paths.artifacts / "validation_report.json")

        failed = [c for c in checks if not c["passed"] and c["severity"] == "error"]
        for c in checks:
            (logger.info if c["passed"] else logger.error)(
                f"[{'PASS' if c['passed'] else 'FAIL'}] {c['check']} = {c['value']:.6g} (limit {c['limit']:.6g})")
        if failed:
            raise CustomException(
                ValueError(f"{len(failed)} validation check(s) failed, see artifacts/validation_report.json"), sys)
        return report