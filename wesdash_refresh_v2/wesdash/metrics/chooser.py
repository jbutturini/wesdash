from __future__ import annotations

from typing import Dict, List

import pandas as pd


def _longify(df: pd.DataFrame, metric_cols: List[str], time_cols: List[str]) -> pd.DataFrame:
    base_cols = ["zcta5", "state_fips", "county_fips"] + time_cols + ["source_name", "source_refresh_cadence", "geo_method"]
    return df.melt(id_vars=base_cols, value_vars=metric_cols, var_name="metric", value_name="value")


def build_chooser(acs5: pd.DataFrame, acs1_alloc: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}
    metrics = ["public_enrolled_3_14", "private_enrolled_3_14", "private_chooser_rate_3_14"]

    if not acs5.empty:
        tables["chooser_acs5"] = _longify(acs5, metrics, ["year"])
    if not acs1_alloc.empty:
        tables["chooser_acs1"] = _longify(acs1_alloc, metrics, ["year"])

    return tables
