from __future__ import annotations

from typing import List

import pandas as pd


def _longify(df: pd.DataFrame, metric_cols: List[str], time_cols: List[str]) -> pd.DataFrame:
    base_cols = ["zcta5", "state_fips", "county_fips"] + time_cols + ["source_name", "source_refresh_cadence", "geo_method"]
    return df.melt(id_vars=base_cols, value_vars=metric_cols, var_name="metric", value_name="value")


def build_households(acs5: pd.DataFrame, acs1_alloc: pd.DataFrame) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    metrics = ["hh_own_children_u18", "hhkids_income_150_plus", "hhkids_income_200_plus"]

    if not acs5.empty:
        frames.append(_longify(acs5, metrics, ["year"]))

    if not acs1_alloc.empty:
        frames.append(_longify(acs1_alloc, metrics, ["year"]))

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
