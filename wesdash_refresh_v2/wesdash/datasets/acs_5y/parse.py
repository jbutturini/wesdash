from __future__ import annotations

import json
from typing import Any, Dict, List

import pandas as pd

from wesdash.geo.zcta import attach_geo_ids
from .schema import DATASET


def _to_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def parse(cfg: Dict[str, Any], raw_files: List[str]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for path in raw_files:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        year = payload["year"]
        header = payload["header"]
        rows = payload["rows"]
        income_vars = payload.get("income_vars", {})
        df = pd.DataFrame(rows, columns=header)
        df["zcta5"] = df["zip code tabulation area"].astype(str).str.zfill(5)
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
        denom = df["public_enrolled_3_14"] + df["private_enrolled_3_14"]
        df["private_chooser_rate_3_14"] = df["private_enrolled_3_14"] / denom.replace(0, pd.NA)

        income_150 = income_vars.get("income_150", [])
        income_200 = income_vars.get("income_200", [])
        df["hhkids_income_150_199"] = df[income_150].sum(axis=1) if income_150 else 0
        df["hhkids_income_200_plus"] = df[income_200].sum(axis=1) if income_200 else 0
        df["hhkids_income_150_plus"] = df["hhkids_income_150_199"] + df["hhkids_income_200_plus"]

        out = df[[
            "zcta5",
            "population_total",
            "age0_4",
            "age5_9",
            "age10_14",
            "hh_own_children_u18",
            "hhkids_income_150_plus",
            "hhkids_income_200_plus",
            "public_enrolled_3_14",
            "private_enrolled_3_14",
            "private_chooser_rate_3_14",
        ]].copy()
        out["year"] = year
        out["source_name"] = DATASET["source_name"]
        out["source_refresh_cadence"] = DATASET["source_refresh_cadence"]
        out["geo_method"] = DATASET["geo_method"]
        target_zctas = cfg["geography"].get("target_zctas") or cfg["geography"]["target_zips"]
        target_zctas = [str(z).zfill(5) for z in target_zctas]
        out = out[out["zcta5"].isin(target_zctas)]
        out = attach_geo_ids(out, cfg["paths"]["geo_cache_dir"], target_zctas)
        frames.append(out)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
