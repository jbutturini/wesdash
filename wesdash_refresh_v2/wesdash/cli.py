from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Dict

import pandas as pd

from wesdash import config as config_mod
from wesdash.excel.build_workbook import build_data_dictionary, build_workbook
from wesdash.geo.zcta import normalize_target_zctas
from wesdash.io.cache import processed_dir, write_parquet
from wesdash.datasets.acs_5y import fetch as acs5_fetch
from wesdash.datasets.acs_5y import parse as acs5_parse
from wesdash.datasets.acs_5y.schema import DATASET as ACS5_SCHEMA
from wesdash.datasets.acs_1y_allocated import fetch as acs1_fetch
from wesdash.datasets.acs_1y_allocated import parse as acs1_parse
from wesdash.datasets.acs_1y_allocated.schema import DATASET as ACS1_SCHEMA
from wesdash.datasets.housing_zip import fetch as housing_fetch
from wesdash.datasets.housing_zip import parse as housing_parse
from wesdash.datasets.housing_zip.schema import DATASET as HOUSING_SCHEMA
from wesdash.datasets.usps_activity import fetch as usps_fetch
from wesdash.datasets.usps_activity import parse as usps_parse
from wesdash.datasets.usps_activity.schema import DATASET as USPS_SCHEMA
from wesdash.datasets.dc_open_data import fetch as dc_fetch
from wesdash.datasets.dc_open_data import parse as dc_parse
from wesdash.datasets.dc_open_data.schema import DATASET as DC_SCHEMA
from wesdash.datasets.osse import fetch as osse_fetch
from wesdash.datasets.osse import parse as osse_parse
from wesdash.datasets.osse.schema import DATASET as OSSE_SCHEMA
from wesdash.datasets.msde_md import fetch as msde_fetch
from wesdash.datasets.msde_md import parse as msde_parse
from wesdash.datasets.msde_md.schema import DATASET as MSDE_SCHEMA
from wesdash.metrics.pipeline import build_pipeline
from wesdash.metrics.households import build_households
from wesdash.metrics.chooser import build_chooser
from wesdash.metrics.public_alternatives import build_public_alternatives


logger = logging.getLogger("wesdash")


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _run_dataset(cfg: Dict, name: str, fetch_fn, parse_fn, required: bool = True) -> tuple[pd.DataFrame, bool]:
    logger.info("Fetching %s", name)
    try:
        raw_files = fetch_fn(cfg)
    except ValueError as exc:
        if not required:
            logger.warning("Skipping %s: %s", name, exc)
            return pd.DataFrame(), True
        raise
    logger.info("Parsing %s", name)
    df = parse_fn(cfg, raw_files)
    if not df.empty:
        out_dir = processed_dir(cfg, name)
        out_path = f"{out_dir}/{name}.parquet"
        write_parquet(df, out_path)
        logger.info("Wrote %s", out_path)
    else:
        logger.warning("No rows produced for %s", name)
    return df, False


def _smoke_check(df: pd.DataFrame, target_zctas: list, name: str) -> None:
    if df.empty:
        raise RuntimeError(f"Smoke check failed: {name} produced empty table")
    if "zcta5" not in df.columns:
        raise RuntimeError(f"Smoke check failed: {name} missing zcta5")
    missing = set(target_zctas) - set(df["zcta5"].dropna().unique())
    if missing:
        logger.warning("%s missing target zctas: %s", name, sorted(missing))
    if "geo_method" not in df.columns:
        raise RuntimeError(f"Smoke check failed: {name} missing geo_method")


def refresh(cfg_path: str) -> None:
    cfg = config_mod.load_config(cfg_path)
    cfg["env"] = dict(os.environ)
    if not cfg["datasets"].get("dc_open_data", {}).get("app_token") and os.getenv("DC_OPEN_DATA_APP_TOKEN"):
        cfg["datasets"]["dc_open_data"]["app_token"] = os.getenv("DC_OPEN_DATA_APP_TOKEN")

    target_zctas = normalize_target_zctas(cfg["geography"]["target_zips"], cfg["geography"]["zip_to_zcta_overrides"])
    cfg["geography"]["target_zctas"] = target_zctas

    Path(cfg["paths"]["raw_dir"]).mkdir(parents=True, exist_ok=True)
    Path(cfg["paths"]["processed_dir"]).mkdir(parents=True, exist_ok=True)
    Path(Path(cfg["paths"]["output_excel"]).parent).mkdir(parents=True, exist_ok=True)

    acs1_alloc, acs1_skipped = _run_dataset(cfg, "acs_1y_allocated", acs1_fetch.fetch, acs1_parse.parse)
    acs5, acs5_skipped = _run_dataset(cfg, "acs_5y", acs5_fetch.fetch, acs5_parse.parse)
    housing, housing_skipped = _run_dataset(cfg, "housing_zip", housing_fetch.fetch, housing_parse.parse, required=False)
    usps, usps_skipped = _run_dataset(cfg, "usps_activity", usps_fetch.fetch, usps_parse.parse, required=False)
    dc_open, dc_open_skipped = _run_dataset(cfg, "dc_open_data", dc_fetch.fetch, dc_parse.parse, required=False)
    osse, osse_skipped = _run_dataset(cfg, "osse", osse_fetch.fetch, osse_parse.parse, required=False)
    msde, msde_skipped = _run_dataset(cfg, "msde_md", msde_fetch.fetch, msde_parse.parse, required=False)

    dataset_status = {
        "acs_5y": (acs5, acs5_skipped),
        "housing_zip": (housing, housing_skipped),
        "usps_activity": (usps, usps_skipped),
        "dc_open_data": (dc_open, dc_open_skipped),
        "osse": (osse, osse_skipped),
        "msde_md": (msde, msde_skipped),
        "acs_1y_allocated": (acs1_alloc, acs1_skipped),
    }
    for name, (df, skipped) in dataset_status.items():
        if skipped:
            continue
        _smoke_check(df, target_zctas, name)

    pipeline_tables = build_pipeline(acs5, acs1_alloc, housing, usps, dc_open)
    households_tables = build_households(acs5, acs1_alloc)
    chooser_tables = build_chooser(acs5, acs1_alloc)
    public_alt = build_public_alternatives(osse, msde)

    if osse_skipped and msde_skipped:
        logger.warning("Skipping public_alternatives smoke check: osse and msde_md not configured.")
    else:
        _smoke_check(public_alt, target_zctas, "public_alternatives")
    for name, df in households_tables.items():
        _smoke_check(df, target_zctas, name)
    for name, df in chooser_tables.items():
        _smoke_check(df, target_zctas, name)
    for name, df in pipeline_tables.items():
        _smoke_check(df, target_zctas, name)

    data_dict = build_data_dictionary([
        ACS5_SCHEMA,
        ACS1_SCHEMA,
        HOUSING_SCHEMA,
        USPS_SCHEMA,
        DC_SCHEMA,
        OSSE_SCHEMA,
        MSDE_SCHEMA,
    ])

    build_workbook(
        cfg["paths"]["output_excel"],
        pipeline_tables,
        households_tables,
        chooser_tables,
        public_alt,
        data_dict,
    )
    logger.info("Wrote workbook %s", cfg["paths"]["output_excel"])


def main() -> None:
    _setup_logging()
    parser = argparse.ArgumentParser(prog="wesdash", description="WES Board Market Dashboard refresh pipeline.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    refresh_cmd = sub.add_parser("refresh", help="Refresh all datasets and rebuild output workbook.")
    refresh_cmd.add_argument("--config", required=True, help="Path to config YAML")

    args = parser.parse_args()
    if args.cmd == "refresh":
        refresh(args.config)


if __name__ == "__main__":
    main()
