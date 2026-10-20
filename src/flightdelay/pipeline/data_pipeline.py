import argparse

from flightdelay.components.data_eda import DataEDA
from flightdelay.components.data_ingestion import DataIngestion
from flightdelay.components.data_transformation import DataTransformation
from flightdelay.components.data_validation import DataValidation
from flightdelay.utils.common import Paths, month_range, read_yaml
from flightdelay.utils.logger import logger

STAGES = ["ingest", "validate", "transform", "eda"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="configs/config.yaml")
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--stages", default="all", help="comma list of: ingest,validate,transform,eda")
    p.add_argument("--force", action="store_true")
    a = p.parse_args()

    cfg = read_yaml(a.config)
    paths = Paths.from_config(cfg)
    months = month_range(a.start or cfg["data"]["start"], a.end or cfg["data"]["end"])
    stages = STAGES if a.stages == "all" else a.stages.split(",")
    logger.info(f"Months: {months[0]} .. {months[-1]} ({len(months)}), stages: {stages}")

    if "ingest" in stages:
        ing = DataIngestion(cfg, paths)
        ing.run_airports(force=a.force)
        ing.run_flights(months, force=a.force)
    if "validate" in stages:
        DataValidation(cfg, paths).run(months)
    if "transform" in stages:
        DataTransformation(cfg, paths).run(months, force=a.force)
    if "eda" in stages:
        DataEDA(cfg, paths).run(months)
    logger.info("Pipeline finished")


if __name__ == "__main__":
    main()