from __future__ import annotations

from typing import List

import pandas as pd


def _longify(df: pd.DataFrame, metric_cols: List[str], time_cols: List[str]) -> pd.DataFrame:
    base_cols = ["zcta5", "state_fips", "county_fips"] + time_cols + ["source_name", "source_refresh_cadence", "geo_method"]
    return df.melt(id_vars=base_cols, value_vars=metric_cols, var_name="metric", value_name="value")


def build_public_alternatives(osse: pd.DataFrame, msde: pd.DataFrame) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    metrics = ["chronic_absenteeism_rate"]

    if not osse.empty:
        frames.append(_longify(osse, metrics, ["year"]))
    if not msde.empty:
        frames.append(_longify(msde, metrics, ["year"]))

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
