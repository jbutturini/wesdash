from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import geopandas as gpd
import pandas as pd

from wesdash.geo import tiger


def zip_to_zcta(zip_code: str, overrides: Dict[str, str]) -> str:
    z = str(zip_code).zfill(5)
    return str(overrides.get(z, z)).zfill(5)


def normalize_target_zctas(target_zips: List[str], overrides: Dict[str, str]) -> List[str]:
    zctas = [zip_to_zcta(z, overrides) for z in target_zips]
    return sorted(set(zctas))


def zcta_state_map(cache_dir: str, target_zctas: List[str], state_fips: Optional[List[str]] = None) -> Dict[str, str]:
    out_dir = Path(cache_dir) / "crosswalks"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "zcta_state_map.csv"

    df = pd.DataFrame()
    if out_path.exists():
        df = pd.read_csv(out_path, dtype={"zcta5": str, "state_fips": str})

    missing = set(target_zctas) - set(df["zcta5"].astype(str)) if target_zctas else set()
    if df.empty or missing:
        zcta_gdf = tiger.load_zcta(cache_dir)
        if target_zctas:
            zcta_gdf = zcta_gdf[zcta_gdf["zcta5"].isin(target_zctas)]
        if state_fips is None:
            state_fips = [tiger.STATE_FIPS["DC"], tiger.STATE_FIPS["MD"]]
        county_gdf = tiger.load_counties(cache_dir, state_fips)
        zcta = zcta_gdf.to_crs("EPSG:5070")
        county = county_gdf.to_crs("EPSG:5070")
        overlay = gpd.overlay(zcta, county, how="intersection", keep_geom_type=False)
        overlay["area"] = overlay.geometry.area
        overlay = overlay[overlay["area"] > 0].copy()
        overlay = overlay.groupby(["zcta5", "state_fips"], as_index=False)["area"].sum()
        overlay = overlay.sort_values(["zcta5", "area"], ascending=[True, False])
        df = overlay.drop_duplicates(subset=["zcta5"])[["zcta5", "state_fips"]]
        df.to_csv(out_path, index=False)

    df = df[df["zcta5"].isin(target_zctas)] if target_zctas else df
    df["zcta5"] = df["zcta5"].astype(str).str.zfill(5)
    return dict(zip(df["zcta5"], df["state_fips"]))


def zcta_county_map(cache_dir: str, target_zctas: List[str], state_fips: Optional[List[str]] = None) -> Dict[str, str]:
    out_dir = Path(cache_dir) / "crosswalks"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "zcta_county_map.csv"

    df = pd.DataFrame()
    if out_path.exists():
        df = pd.read_csv(out_path, dtype={"zcta5": str, "county_fips": str})

    missing = set(target_zctas) - set(df["zcta5"].astype(str)) if target_zctas else set()
    if df.empty or missing:
        zcta_gdf = tiger.load_zcta(cache_dir)
        if target_zctas:
            zcta_gdf = zcta_gdf[zcta_gdf["zcta5"].isin(target_zctas)]
        if state_fips is None:
            state_fips = [tiger.STATE_FIPS["DC"], tiger.STATE_FIPS["MD"]]
        county_gdf = tiger.load_counties(cache_dir, state_fips)
        zcta = zcta_gdf.to_crs("EPSG:5070")
        county = county_gdf.to_crs("EPSG:5070")
        overlay = gpd.overlay(zcta, county, how="intersection", keep_geom_type=False)
        overlay["area"] = overlay.geometry.area
        overlay = overlay[overlay["area"] > 0].copy()
        overlay = overlay.groupby(["zcta5", "county_fips"], as_index=False)["area"].sum()
        overlay = overlay.sort_values(["zcta5", "area"], ascending=[True, False])
        df = overlay.drop_duplicates(subset=["zcta5"])[["zcta5", "county_fips"]]
        df.to_csv(out_path, index=False)

    df = df[df["zcta5"].isin(target_zctas)] if target_zctas else df
    df["zcta5"] = df["zcta5"].astype(str).str.zfill(5)
    return dict(zip(df["zcta5"], df["county_fips"]))


def attach_geo_ids(df: pd.DataFrame, cache_dir: str, target_zctas: List[str]) -> pd.DataFrame:
    state_map = zcta_state_map(cache_dir, target_zctas)
    county_map = zcta_county_map(cache_dir, target_zctas)
    df["state_fips"] = df["zcta5"].map(state_map)
    df["county_fips"] = df["zcta5"].map(county_map)
    return df
