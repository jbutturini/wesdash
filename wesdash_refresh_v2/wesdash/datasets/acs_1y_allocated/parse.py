from __future__ import annotations

import json
from typing import Any, Dict, List

import pandas as pd

from wesdash.geo import crosswalks
from wesdash.geo import tiger
from wesdash.geo.zcta import attach_geo_ids
from wesdash.io.cache import latest_processed_dir
from .schema import DATASET


def _to_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _load_latest_acs5(cfg: Dict[str, Any]) -> pd.DataFrame:
    latest_dir = latest_processed_dir(cfg, "acs_5y")
    if latest_dir is None:
        raise RuntimeError("ACS 5-year processed data not found; run acs_5y first.")
    path = f"{latest_dir}/acs_5y.parquet"
    return pd.read_parquet(path)


def _build_weights(cfg: Dict[str, Any], zcta_list: List[str]) -> pd.DataFrame:
    cache_dir = cfg["paths"]["geo_cache_dir"]
    zcta_gdf = tiger.load_zcta(cache_dir)
    zcta_gdf = zcta_gdf[zcta_gdf["zcta5"].isin(zcta_list)]
    counties = tiger.load_counties(cache_dir, [tiger.STATE_FIPS["DC"], tiger.STATE_FIPS["MD"]])

    area_weights = crosswalks.county_zcta_area_weights(zcta_gdf, counties)
    acs5 = _load_latest_acs5(cfg)
    pop = acs5[["zcta5", "population_total"]].copy()
    weights = crosswalks.weights_from_population(area_weights, pop, "county_fips", "population_total")
    return weights


def parse(cfg: Dict[str, Any], raw_files: List[str]) -> pd.DataFrame:
    target_zctas = cfg["geography"].get("target_zctas") or cfg["geography"]["target_zips"]
    target_zctas = [str(z).zfill(5) for z in target_zctas]
    weights = _build_weights(cfg, target_zctas)

    frames: List[pd.DataFrame] = []
    for path in raw_files:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        year = payload["year"]
        header = payload["header"]
        rows = payload["rows"]
        income_vars = payload.get("income_vars", {})

        df = pd.DataFrame(rows, columns=header)
        df["county_fips"] = df["state"] + df["county"]
        numeric_cols = [c for c in header if c.endswith("E")]
        df = _to_numeric(df, numeric_cols)

        df["population_total"] = df["B01001_001E"]
        df["age0_4"] = df["B01001_003E"] + df["B01001_027E"]
        df["age5_9"] = df["B01001_004E"] + df["B01001_028E"]
        df["age10_14"] = df["B01001_005E"] + df["B01001_029E"]
        df["hh_own_children_u18"] = df["S1101_C01_005E"]

        pub_cols = ["B14003_004E", "B14003_005E", "B14003_006E", "B14003_032E", "B14003_033E", "B14003_034E"]
        priv_cols = ["B14003_013E", "B14003_014E", "B14003_015E", "B14003_041E", "B14003_042E", "B14003_043E"]
        df["public_enrolled_3_14"] = df[pub_cols].sum(axis=1)
        df["private_enrolled_3_14"] = df[priv_cols].sum(axis=1)
        income_150 = income_vars.get("income_150", [])
        income_200 = income_vars.get("income_200", [])
        df["hhkids_income_150_199"] = df[income_150].sum(axis=1) if income_150 else 0
        df["hhkids_income_200_plus"] = df[income_200].sum(axis=1) if income_200 else 0
        df["hhkids_income_150_plus"] = df["hhkids_income_150_199"] + df["hhkids_income_200_plus"]

        keep_cols = [
            "county_fips",
            "population_total",
            "age0_4",
            "age5_9",
            "age10_14",
            "hh_own_children_u18",
            "hhkids_income_150_plus",
            "hhkids_income_200_plus",
            "public_enrolled_3_14",
            "private_enrolled_3_14",
        ]
        df = df[keep_cols].copy()
        df["year"] = year

        value_cols = [c for c in df.columns if c not in ("county_fips", "year")]
        allocated = crosswalks.county_to_zcta_weighted(df, weights, value_cols, county_col="county_fips")
        allocated = allocated[allocated["zcta5"].isin(target_zctas)]
        allocated = attach_geo_ids(allocated, cfg["paths"]["geo_cache_dir"], target_zctas)
        denom = allocated["public_enrolled_3_14"] + allocated["private_enrolled_3_14"]
        allocated["private_chooser_rate_3_14"] = allocated["private_enrolled_3_14"] / denom.replace(0, pd.NA)
        allocated["source_name"] = DATASET["source_name"]
        allocated["source_refresh_cadence"] = DATASET["source_refresh_cadence"]
        allocated["geo_method"] = DATASET["geo_method"]
        frames.append(allocated)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
