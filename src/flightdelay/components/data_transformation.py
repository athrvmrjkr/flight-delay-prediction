import sys
from typing import List, Tuple

import polars as pl

from flightdelay.utils.common import Paths, partition_file, write_json
from flightdelay.utils.exception import CustomException
from flightdelay.utils.logger import logger


class DataTransformation:
    def __init__(self, cfg: dict, paths: Paths):
        self.cfg = cfg
        self.paths = paths

    def _process_month(self, airports: pl.DataFrame, year: int, month: int) -> dict:
        src = partition_file(self.paths.interim, "flights", year, month)
        out = partition_file(self.paths.processed, "flights", year, month)
        df = pl.read_parquet(src)
        report = {"year": year, "month": month, "rows_in": df.height}

        # 1. keep only flights with a valid target
        df = df.filter(
            (pl.col("Cancelled") == 0) & (pl.col("Diverted") == 0)
            & pl.col("ArrDel15").is_not_null()
            & pl.col("CRSDepTime").is_not_null()
            & (pl.col("CRSElapsedTime") > 0)
        )
        report["rows_after_target_filter"] = df.height

        # 2. clean tail numbers
        df = df.with_columns(
            pl.col("Tail_Number").str.to_uppercase().alias("Tail_Number")
        )

        # 3. attach airport coordinates and timezones
        o = airports.select(pl.col("iata_code").alias("Origin"),
                            pl.col("tz").alias("origin_tz"),
                            pl.col("latitude_deg").alias("origin_lat"),
                            pl.col("longitude_deg").alias("origin_lon"))
        d = airports.select(pl.col("iata_code").alias("Dest"),
                            pl.col("tz").alias("dest_tz"),
                            pl.col("latitude_deg").alias("dest_lat"),
                            pl.col("longitude_deg").alias("dest_lon"))
        df = df.join(o, on="Origin", how="inner").join(d, on="Dest", how="inner")
        report["rows_after_airport_join"] = df.height

        # 4. scheduled departure: local clock time (hhmm; 2400 rolls to next day) -> UTC
        minutes = (pl.col("CRSDepTime") // 100) * 60 + (pl.col("CRSDepTime") % 100)
        local = pl.col("FlightDate").cast(pl.Datetime("us")) + pl.duration(minutes=minutes)
        parts = []
        for tz in df["origin_tz"].unique().to_list():
            sub = df.filter(pl.col("origin_tz") == tz).with_columns(local.alias("sched_dep_local"))
            sub = sub.with_columns(
                pl.col("sched_dep_local")
                .dt.replace_time_zone(tz, ambiguous="earliest", non_existent="null")
                .dt.convert_time_zone("UTC")
                .alias("sched_dep_utc")
            )
            parts.append(sub)
        df = pl.concat(parts).filter(pl.col("sched_dep_utc").is_not_null())
        report["rows_after_time_conversion"] = df.height

        # 5. scheduled arrival (UTC) = departure + scheduled duration; simple local features
        df = df.with_columns(
            (pl.col("sched_dep_utc") + pl.duration(minutes=pl.col("CRSElapsedTime").cast(pl.Int64)))
            .alias("sched_arr_utc"),
            pl.col("sched_dep_local").dt.hour().cast(pl.Int8).alias("dep_hour_local"),
            pl.col("sched_dep_local").dt.weekday().cast(pl.Int8).alias("dep_dow_local"),
        ).sort("sched_dep_utc")

        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(".tmp")
        df.write_parquet(tmp, compression="zstd")
        tmp.replace(out)
        report["rows_out"] = df.height
        logger.info(f"{year}-{month:02d}: {report['rows_in']:,} -> {df.height:,} rows -> {out}")
        return report

    def run(self, months: List[Tuple[int, int]], force: bool = False) -> list:
        try:
            airports = pl.read_parquet(self.paths.reference / "airports.parquet")
            reports = []
            for y, m in months:
                if not partition_file(self.paths.interim, "flights", y, m).exists():
                    logger.warning(f"{y}-{m:02d}: no interim data, skipping")
                    continue
                if partition_file(self.paths.processed, "flights", y, m).exists() and not force:
                    logger.info(f"{y}-{m:02d}: already processed, skipping")
                    continue
                reports.append(self._process_month(airports, y, m))
            write_json(reports, self.paths.artifacts / "transform_report.json")
            return reports
        except Exception as e:
            raise CustomException(e, sys)