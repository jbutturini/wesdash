from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from wesdash.geo.zcta import attach_geo_ids
from .schema import DATASET


def _parse_file(path: str, metric: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "RegionName" not in df.columns:
        raise ValueError(f"Missing RegionName in {path}")
    id_cols = [
        "RegionName",
        "RegionID",
        "SizeRank",
        "RegionType",
        "StateName",
        "State",
        "City",
        "Metro",
        "CountyName",
    ]
    id_cols = [c for c in id_cols if c in df.columns]
    value_cols = [c for c in df.columns if c not in id_cols]
    long_df = df.melt(id_vars=id_cols, value_vars=value_cols, var_name="period_start", value_name=metric)
    long_df["period_start"] = pd.to_datetime(long_df["period_start"], errors="coerce")
    long_df["zcta5"] = long_df["RegionName"].astype(str).str.zfill(5)
    out = long_df[["zcta5", "period_start", metric]].copy()
    return out


def parse(cfg: Dict[str, Any], raw_files: List[str]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for path in raw_files:
        metric = Path(path).stem
        frames.append(_parse_file(path, metric))

    if not frames:
        return pd.DataFrame()

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on=["zcta5", "period_start"], how="outer")

    target_zctas = cfg["geography"].get("target_zctas") or cfg["geography"]["target_zips"]
    target_zctas = [str(z).zfill(5) for z in target_zctas]
    merged = merged[merged["zcta5"].isin(target_zctas)]
    merged = merged[merged["period_start"] >= pd.Timestamp(2015, 1, 1)]
    merged = attach_geo_ids(merged, cfg["paths"]["geo_cache_dir"], target_zctas)
    merged["source_name"] = DATASET["source_name"]
    merged["source_refresh_cadence"] = DATASET["source_refresh_cadence"]
    merged["geo_method"] = DATASET["geo_method"]
    return merged
