from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from wesdash.geo import crosswalks
from wesdash.geo import tiger
from wesdash.geo.zcta import attach_geo_ids
from .schema import DATASET


def _build_weights(cfg: Dict[str, Any], zcta_list: List[str]) -> pd.DataFrame:
    cache_dir = cfg["paths"]["geo_cache_dir"]
    zcta_gdf = tiger.load_zcta(cache_dir)
    zcta_gdf = zcta_gdf[zcta_gdf["zcta5"].isin(zcta_list)]

    tract_frames = []
    for state in ["DC", "MD"]:
        tract_gdf = tiger.load_tracts(cache_dir, tiger.STATE_FIPS[state])
        tract_frames.append(tract_gdf)
    tract_gdf = pd.concat(tract_frames, ignore_index=True)
    tract_gdf = tract_gdf.set_geometry("geometry")

    weights = crosswalks.tract_zcta_area_weights(zcta_gdf, tract_gdf)
    return weights


def parse(cfg: Dict[str, Any], raw_files: List[str]) -> pd.DataFrame:
    ds_cfg = cfg["datasets"].get("usps_activity", {})
    tract_field = ds_cfg.get("tract_field", "tract_fips")
    date_field = ds_cfg.get("date_field")
    year_field = ds_cfg.get("year_field", "year")
    month_field = ds_cfg.get("month_field", "month")
    value_field = ds_cfg.get("value_field", "active_address_count")

    target_zctas = cfg["geography"].get("target_zctas") or cfg["geography"]["target_zips"]
    target_zctas = [str(z).zfill(5) for z in target_zctas]
    weights = _build_weights(cfg, target_zctas)

    frames: List[pd.DataFrame] = []
    for path in raw_files:
        df = pd.read_csv(path)
        if date_field and date_field in df.columns:
            df["period_start"] = pd.to_datetime(df[date_field], errors="coerce")
        else:
            df["period_start"] = pd.to_datetime(df[year_field].astype(str) + "-" + df[month_field].astype(str) + "-01", errors="coerce")
        df["tract_fips"] = df[tract_field].astype(str).str.zfill(11)
        df[value_field] = pd.to_numeric(df[value_field], errors="coerce")
        df = df[["tract_fips", "period_start", value_field]].copy()

        merged = df.merge(weights, on="tract_fips", how="left")
        merged[value_field] = merged[value_field] * merged["weight"]
        out = merged.groupby(["zcta5", "period_start"], as_index=False)[value_field].sum()
        out = out[out["zcta5"].isin(target_zctas)]
        out = attach_geo_ids(out, cfg["paths"]["geo_cache_dir"], target_zctas)
        out["source_name"] = DATASET["source_name"]
        out["source_refresh_cadence"] = DATASET["source_refresh_cadence"]
        out["geo_method"] = DATASET["geo_method"]
        frames.append(out)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
