from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from wesdash.geo import spatial
from wesdash.geo import tiger
from wesdash.geo.zcta import attach_geo_ids
from .schema import DATASET


def _extract_lat_lon(df: pd.DataFrame, lat_field: str, lon_field: str) -> pd.DataFrame:
    if lat_field in df.columns and lon_field in df.columns:
        return df
    if "location" in df.columns:
        lat = df["location"].apply(lambda x: x.get("latitude") if isinstance(x, dict) else None)
        lon = df["location"].apply(lambda x: x.get("longitude") if isinstance(x, dict) else None)
        df[lat_field] = pd.to_numeric(lat, errors="coerce")
        df[lon_field] = pd.to_numeric(lon, errors="coerce")
    return df


def parse(cfg: Dict[str, Any], raw_files: List[str]) -> pd.DataFrame:
    ds_cfg = cfg["datasets"].get("dc_open_data", {})
    datasets = ds_cfg.get("datasets", [])
    if not datasets:
        return pd.DataFrame()

    dataset_map = {}
    for ds in datasets:
        name = ds.get("name", ds["dataset_id"])
        dataset_map[name] = ds

    cache_dir = cfg["paths"]["geo_cache_dir"]
    zcta_gdf = tiger.load_zcta(cache_dir)

    frames: List[pd.DataFrame] = []
    for path in raw_files:
        name = Path(path).stem
        ds = dataset_map.get(name)
        if not ds:
            continue
        date_field = ds.get("date_field", "issue_date")
        zip_field = ds.get("zip_field")
        lat_field = ds.get("lat_field", "latitude")
        lon_field = ds.get("lon_field", "longitude")
        value_field = ds.get("value_field")

        with open(path, "r", encoding="utf-8") as f:
            rows = json.load(f)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["period_start"] = pd.to_datetime(df[date_field], errors="coerce").dt.to_period("M").dt.to_timestamp()
        if zip_field and zip_field in df.columns:
            df["zcta5"] = df[zip_field].astype(str).str.zfill(5)
            geo_method = "native_zip"
        else:
            df = _extract_lat_lon(df, lat_field, lon_field)
            df = spatial.points_to_zcta(df, lat_field, lon_field, zcta_gdf)
            geo_method = DATASET["geo_method"]

        df = df.dropna(subset=["zcta5", "period_start"])
        df["record_count"] = 1
        agg_cols = ["zcta5", "period_start"]
        agg_map = {"record_count": "sum"}
        if value_field and value_field in df.columns:
            df[value_field] = pd.to_numeric(df[value_field], errors="coerce")
            agg_map[value_field] = "sum"
        out = df.groupby(agg_cols, as_index=False).agg(agg_map)
        target_zctas = cfg["geography"].get("target_zctas") or cfg["geography"]["target_zips"]
        target_zctas = [str(z).zfill(5) for z in target_zctas]
        out = out[out["zcta5"].isin(target_zctas)]
        out = attach_geo_ids(out, cfg["paths"]["geo_cache_dir"], target_zctas)
        out["source_name"] = DATASET["source_name"]
        out["source_refresh_cadence"] = DATASET["source_refresh_cadence"]
        out["geo_method"] = geo_method
        frames.append(out)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
