from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from wesdash.geo import spatial
from wesdash.geo import tiger
from wesdash.geo.zcta import attach_geo_ids
from .schema import DATASET


def parse(cfg: Dict[str, Any], raw_files: List[str]) -> pd.DataFrame:
    ds_cfg = cfg["datasets"].get("msde_md", {})
    sheet = ds_cfg.get("sheet")
    zip_field = ds_cfg.get("zip_field")
    lat_field = ds_cfg.get("lat_field")
    lon_field = ds_cfg.get("lon_field")
    rate_field = ds_cfg.get("rate_field", "chronic_absenteeism_rate")
    weight_field = ds_cfg.get("weight_field")
    year_field = ds_cfg.get("year_field", "year")

    cache_dir = cfg["paths"]["geo_cache_dir"]
    zcta_gdf = tiger.load_zcta(cache_dir)

    frames: List[pd.DataFrame] = []
    for path in raw_files:
        df = pd.read_excel(path, sheet_name=sheet)
        df[rate_field] = pd.to_numeric(df[rate_field], errors="coerce")
        df[year_field] = pd.to_numeric(df[year_field], errors="coerce").astype("Int64")

        if zip_field and zip_field in df.columns:
            df["zcta5"] = df[zip_field].astype(str).str.zfill(5)
            geo_method = "native_zip"
        elif lat_field and lon_field and lat_field in df.columns and lon_field in df.columns:
            df = spatial.points_to_zcta(df, lat_field, lon_field, zcta_gdf)
            geo_method = DATASET["geo_method"]
        else:
            raise ValueError("MSDE data must include zip_field or lat/lon fields")

        df = df.dropna(subset=["zcta5", year_field])
        if weight_field and weight_field in df.columns:
            df[weight_field] = pd.to_numeric(df[weight_field], errors="coerce")
            df["weighted_rate"] = df[rate_field] * df[weight_field]
            grouped = df.groupby(["zcta5", year_field], as_index=False).agg({"weighted_rate": "sum", weight_field: "sum"})
            grouped["chronic_absenteeism_rate"] = grouped["weighted_rate"] / grouped[weight_field].replace(0, pd.NA)
            out = grouped[["zcta5", year_field, "chronic_absenteeism_rate"]].copy()
        else:
            out = df.groupby(["zcta5", year_field], as_index=False)[rate_field].mean()
            out = out.rename(columns={rate_field: "chronic_absenteeism_rate"})

        out = out.rename(columns={year_field: "year"})
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
