import shutil
import sys
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

import polars as pl
import requests
from requests.adapters import HTTPAdapter
from timezonefinder import TimezoneFinder
from urllib3.util.retry import Retry

from flightdelay.constants import (
    DATE_COL, FLOAT32_COLS, INT8_COLS, INT32_COLS, RAW_COLUMNS, STRING_COLS,
)
from flightdelay.utils.common import Paths, partition_file, sha256_of, write_json
from flightdelay.utils.exception import CustomException
from flightdelay.utils.logger import logger


class DataIngestion:
    def __init__(self, cfg: dict, paths: Paths):
        self.cfg = cfg
        self.paths = paths
        self.bts = cfg["bts"]

    # ---------- HTTP ----------
    def _session(self) -> requests.Session:
        s = requests.Session()
        retry = Retry(total=self.bts["retries"], backoff_factor=2,
                      status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
        s.mount("https://", HTTPAdapter(max_retries=retry))
        return s

    def _download_zip(self, year: int, month: int) -> dict:
        url = self.bts["url_template"].format(year=year, month=month)
        dest = self.paths.raw / "bts" / f"{year}_{month:02d}.zip"
        dest.parent.mkdir(parents=True, exist_ok=True)

        if dest.exists() and zipfile.is_zipfile(dest):
            return {"url": url, "path": str(dest), "status": "cached"}

        part = dest.with_suffix(".part")
        with self._session().get(url, stream=True, timeout=self.bts["timeout_seconds"],
                                 verify=self.bts["verify_ssl"]) as r:
            if r.status_code == 404:
                logger.warning(f"{year}-{month:02d} not published yet (404)")
                return {"url": url, "status": "unavailable"}
            r.raise_for_status()
            expected = int(r.headers.get("Content-Length", 0))
            written = 0
            with open(part, "wb") as f:
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
                    written += len(chunk)
        if expected and written != expected:
            part.unlink(missing_ok=True)
            raise IOError(f"Incomplete download {url}: {written}/{expected} bytes")
        if not zipfile.is_zipfile(part):
            part.unlink(missing_ok=True)
            raise IOError(f"Downloaded file is not a valid zip: {url}")
        part.replace(dest)
        logger.info(f"Downloaded {dest.name} ({written / 1e6:.1f} MB)")
        return {"url": url, "path": str(dest), "status": "downloaded"}

    # ---------- zip -> typed parquet ----------
    @staticmethod
    def _typed(lf: pl.LazyFrame) -> pl.LazyFrame:
        def clean_str(c):
            s = pl.col(c).str.strip_chars()
            return pl.when(s == "").then(None).otherwise(s).alias(c)

        return lf.select(
            pl.col(DATE_COL).str.to_date("%Y-%m-%d", strict=False),
            *[clean_str(c) for c in STRING_COLS],
            *[pl.col(c).cast(pl.Float64, strict=False).cast(pl.Int32, strict=False).alias(c)
              for c in INT32_COLS],
            *[pl.col(c).cast(pl.Float64, strict=False).cast(pl.Int8, strict=False).alias(c)
              for c in INT8_COLS],
            *[pl.col(c).cast(pl.Float32, strict=False).alias(c) for c in FLOAT32_COLS],
        )

    def _zip_to_parquet(self, zip_path: Path, year: int, month: int, force: bool) -> dict:
        out = partition_file(self.paths.interim, "flights", year, month)
        if out.exists() and not force:
            rows = pl.scan_parquet(out).select(pl.len()).collect().item()
            return {"rows": rows, "converted": False}

        out.parent.mkdir(parents=True, exist_ok=True)
        tmp_dir = Path(tempfile.mkdtemp(prefix="bts_"))
        try:
            with zipfile.ZipFile(zip_path) as z:
                csv_names = [n for n in z.namelist() if n.lower().endswith(".csv")]
                if not csv_names:
                    raise IOError(f"No CSV inside {zip_path}")
                csv_path = Path(z.extract(csv_names[0], tmp_dir))

            lf = pl.scan_csv(csv_path, infer_schema_length=0, encoding="utf8-lossy")
            missing = set(RAW_COLUMNS) - set(lf.collect_schema().names())
            if missing:
                raise ValueError(f"BTS file schema changed, missing columns: {sorted(missing)}")

            tmp_out = out.with_suffix(".tmp")
            self._typed(lf).sink_parquet(tmp_out, compression="zstd")
            tmp_out.replace(out)
            rows = pl.scan_parquet(out).select(pl.len()).collect().item()
            logger.info(f"{year}-{month:02d}: wrote {rows:,} rows -> {out}")
            return {"rows": rows, "converted": True}
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def _process_month(self, year: int, month: int, force: bool) -> dict:
        info = {"year": year, "month": month}
        dl = self._download_zip(year, month)
        info.update(dl)
        if dl["status"] == "unavailable":
            return info
        zip_path = Path(dl["path"])
        info["bytes"] = zip_path.stat().st_size
        info["sha256"] = sha256_of(zip_path)
        info.update(self._zip_to_parquet(zip_path, year, month, force))
        return info

    # ---------- public ----------
    def run_flights(self, months: List[Tuple[int, int]], force: bool = False) -> list:
        results = []
        with ThreadPoolExecutor(max_workers=self.bts["max_workers"]) as ex:
            futs = {ex.submit(self._process_month, y, m, force): (y, m) for y, m in months}
            for fut in as_completed(futs):
                y, m = futs[fut]
                try:
                    results.append(fut.result())
                except Exception as e:
                    logger.error(f"{y}-{m:02d} failed: {e}")
                    results.append({"year": y, "month": m, "status": "failed", "error": str(e)})
        results.sort(key=lambda r: (r["year"], r["month"]))
        write_json(results, self.paths.raw / "manifest.json")
        failed = [r for r in results if r["status"] == "failed"]
        if failed:
            raise CustomException(RuntimeError(f"{len(failed)} month(s) failed, see manifest.json"), sys)
        return results

    def run_airports(self, force: bool = False) -> Path:
        out = self.paths.reference / "airports.parquet"
        if out.exists() and not force:
            return out
        try:
            r = self._session().get(self.cfg["airports"]["url"], timeout=60)
            r.raise_for_status()
            raw_csv = self.paths.reference / "airports_raw.csv"
            raw_csv.write_bytes(r.content)

            rank = {"large_airport": 0, "medium_airport": 1, "small_airport": 2}
            cols = ["iata_code", "name", "municipality", "iso_country",
                    "latitude_deg", "longitude_deg"]
            df = (
                pl.read_csv(raw_csv, infer_schema_length=0)
                .filter(pl.col("iata_code").is_not_null() & (pl.col("iata_code") != ""))
                .with_columns(
                    pl.col("latitude_deg").cast(pl.Float64, strict=False),
                    pl.col("longitude_deg").cast(pl.Float64, strict=False),
                    pl.col("type").replace_strict(rank, default=3).alias("_rank"),
                )
                .drop_nulls(["latitude_deg", "longitude_deg"])
                .sort("_rank")
                .unique(subset=["iata_code"], keep="first", maintain_order=True)
                .select(cols)
            )

            overrides = self.cfg["airports"].get("overrides") or []
            if overrides:
                ov = (pl.DataFrame(overrides).select(cols)
                      .with_columns(pl.col("latitude_deg").cast(pl.Float64),
                                    pl.col("longitude_deg").cast(pl.Float64)))
                df = pl.concat([df.filter(~pl.col("iata_code").is_in(ov["iata_code"])), ov])

            tf = TimezoneFinder()
            tz = [tf.timezone_at(lat=la, lng=lo)
                  for la, lo in zip(df["latitude_deg"], df["longitude_deg"])]
            n_before = df.height
            df = df.with_columns(pl.Series("tz", tz)).drop_nulls("tz")
            if df.height < n_before:
                logger.warning(f"{n_before - df.height} airports dropped: no timezone found")
            df.write_parquet(out, compression="zstd")
            logger.info(f"Airports reference: {df.height} airports -> {out}")
            return out
        except Exception as e:
            raise CustomException(e, sys)