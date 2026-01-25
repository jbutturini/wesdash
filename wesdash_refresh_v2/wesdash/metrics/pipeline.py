from __future__ import annotations

from typing import Dict, List

import pandas as pd


def _longify(df: pd.DataFrame, metric_cols: List[str], time_cols: List[str]) -> pd.DataFrame:
    base_cols = ["zcta5", "state_fips", "county_fips"] + time_cols + ["source_name", "source_refresh_cadence", "geo_method"]
    return df.melt(id_vars=base_cols, value_vars=metric_cols, var_name="metric", value_name="value")


def build_pipeline(
    acs5: pd.DataFrame,
    acs1_allocated: pd.DataFrame,
    housing: pd.DataFrame,
    usps: pd.DataFrame,
    dc_open: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}

    if not acs5.empty:
        metrics = ["age0_4", "age5_9", "age10_14"]
        tables["pipeline_acs5"] = _longify(acs5, metrics, ["year"])

    if not acs1_allocated.empty:
        metrics = ["age0_4", "age5_9", "age10_14"]
        tables["pipeline_acs1"] = _longify(acs1_allocated, metrics, ["year"])

    if not housing.empty:
        metrics = [c for c in housing.columns if c not in {"zcta5", "period_start", "source_name", "source_refresh_cadence", "geo_method"}]
        tables["pipeline_housing"] = _longify(housing, metrics, ["period_start"])

    if not usps.empty:
        metrics = [c for c in usps.columns if c not in {"zcta5", "period_start", "source_name", "source_refresh_cadence", "geo_method"}]
        tables["pipeline_usps"] = _longify(usps, metrics, ["period_start"])

    if not dc_open.empty:
        metrics = [c for c in dc_open.columns if c not in {"zcta5", "period_start", "source_name", "source_refresh_cadence", "geo_method"}]
        tables["pipeline_dc_open"] = _longify(dc_open, metrics, ["period_start"])

    return tables
