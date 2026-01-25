from __future__ import annotations

from typing import Iterable, List

import geopandas as gpd
import pandas as pd


def _validate_weights(weights: pd.DataFrame, group_col: str, weight_col: str = "weight") -> None:
    sums = weights.groupby(group_col)[weight_col].sum().round(6)
    if (sums == 0).any():
        raise ValueError(f"Zero-weight groups found for {group_col}")
    if not (sums.between(0.99, 1.01)).all():
        bad = sums[~sums.between(0.99, 1.01)]
        raise ValueError(f"Weights do not sum to 1 for {group_col}: {bad.to_dict()}")


def county_zcta_area_weights(zcta_gdf: gpd.GeoDataFrame, county_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    zcta = zcta_gdf.to_crs("EPSG:5070")
    county = county_gdf.to_crs("EPSG:5070")
    overlay = gpd.overlay(zcta, county, how="intersection")
    overlay["area"] = overlay.geometry.area
    overlay = overlay[overlay["area"] > 0].copy()
    overlay["weight"] = overlay.groupby("county_fips")["area"].transform(lambda x: x / x.sum())
    out = overlay[["county_fips", "zcta5", "weight"]].copy()
    _validate_weights(out, "county_fips", "weight")
    return out


def tract_zcta_area_weights(zcta_gdf: gpd.GeoDataFrame, tract_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    zcta = zcta_gdf.to_crs("EPSG:5070")
    tract = tract_gdf.to_crs("EPSG:5070")
    overlay = gpd.overlay(zcta, tract, how="intersection")
    overlay["area"] = overlay.geometry.area
    overlay = overlay[overlay["area"] > 0].copy()
    overlay["weight"] = overlay.groupby("tract_fips")["area"].transform(lambda x: x / x.sum())
    out = overlay[["tract_fips", "zcta5", "weight"]].copy()
    _validate_weights(out, "tract_fips", "weight")
    return out


def weights_from_population(
    area_weights: pd.DataFrame,
    zcta_pop: pd.DataFrame,
    geo_col: str,
    pop_col: str,
) -> pd.DataFrame:
    merged = area_weights.merge(zcta_pop[["zcta5", pop_col]], on="zcta5", how="left")
    merged[pop_col] = merged[pop_col].fillna(0)
    merged["weighted_pop"] = merged["weight"] * merged[pop_col]
    merged["weight"] = merged.groupby(geo_col)["weighted_pop"].transform(lambda x: x / x.sum() if x.sum() else 0)
    out = merged[[geo_col, "zcta5", "weight"]].copy()
    _validate_weights(out, geo_col, "weight")
    return out


def county_to_zcta_weighted(
    df_county: pd.DataFrame,
    weights_df: pd.DataFrame,
    value_cols: Iterable[str],
    county_col: str = "county_fips",
) -> pd.DataFrame:
    weights = weights_df.copy()
    _validate_weights(weights, county_col, "weight")
    merged = df_county.merge(weights, on=county_col, how="left")
    for col in value_cols:
        merged[col] = merged[col] * merged["weight"]
    group_cols = ["zcta5"] + [c for c in df_county.columns if c not in value_cols and c != county_col]
    out = merged.groupby(group_cols).sum(numeric_only=True).reset_index()
    if "weight" in out.columns:
        out = out.drop(columns=["weight"])
    return out
