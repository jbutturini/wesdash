from __future__ import annotations

import os
import zipfile
from pathlib import Path
from typing import Dict, List

import geopandas as gpd

from wesdash.io.http import download_file

ZCTA_YEAR = 2023
COUNTY_YEAR = 2023
TRACT_YEAR = 2023

STATE_FIPS = {
    "DC": "11",
    "MD": "24",
}


def _download_and_extract(url: str, out_dir: str) -> str:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    zip_path = os.path.join(out_dir, os.path.basename(url))
    if not os.path.exists(zip_path):
        download_file(url, zip_path)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(out_dir)
    return out_dir


def ensure_zcta_shapes(cache_dir: str) -> str:
    out_dir = os.path.join(cache_dir, "tiger", f"zcta_{ZCTA_YEAR}")
    shp_path = os.path.join(out_dir, f"tl_{ZCTA_YEAR}_us_zcta520.shp")
    if os.path.exists(shp_path):
        return shp_path
    url = f"https://www2.census.gov/geo/tiger/TIGER{ZCTA_YEAR}/ZCTA520/tl_{ZCTA_YEAR}_us_zcta520.zip"
    _download_and_extract(url, out_dir)
    return shp_path


def ensure_county_shapes(cache_dir: str) -> str:
    out_dir = os.path.join(cache_dir, "tiger", f"county_{COUNTY_YEAR}")
    shp_path = os.path.join(out_dir, f"tl_{COUNTY_YEAR}_us_county.shp")
    if os.path.exists(shp_path):
        return shp_path
    url = f"https://www2.census.gov/geo/tiger/TIGER{COUNTY_YEAR}/COUNTY/tl_{COUNTY_YEAR}_us_county.zip"
    _download_and_extract(url, out_dir)
    return shp_path


def ensure_tract_shapes(cache_dir: str, state_fips: str) -> str:
    out_dir = os.path.join(cache_dir, "tiger", f"tract_{TRACT_YEAR}_{state_fips}")
    shp_path = os.path.join(out_dir, f"tl_{TRACT_YEAR}_{state_fips}_tract.shp")
    if os.path.exists(shp_path):
        return shp_path
    url = f"https://www2.census.gov/geo/tiger/TIGER{TRACT_YEAR}/TRACT/tl_{TRACT_YEAR}_{state_fips}_tract.zip"
    _download_and_extract(url, out_dir)
    return shp_path


def load_zcta(cache_dir: str) -> gpd.GeoDataFrame:
    shp = ensure_zcta_shapes(cache_dir)
    gdf = gpd.read_file(shp)
    gdf = gdf.rename(columns={"ZCTA5CE20": "zcta5", "ZCTA5CE10": "zcta5"})
    gdf["zcta5"] = gdf["zcta5"].astype(str).str.zfill(5)
    return gdf[["zcta5", "geometry"]]


def load_counties(cache_dir: str, state_fips: List[str]) -> gpd.GeoDataFrame:
    shp = ensure_county_shapes(cache_dir)
    gdf = gpd.read_file(shp)
    gdf = gdf[gdf["STATEFP"].isin(state_fips)].copy()
    gdf["county_fips"] = gdf["STATEFP"] + gdf["COUNTYFP"]
    gdf["state_fips"] = gdf["STATEFP"]
    return gdf[["county_fips", "state_fips", "geometry"]]


def load_tracts(cache_dir: str, state_fips: str) -> gpd.GeoDataFrame:
    shp = ensure_tract_shapes(cache_dir, state_fips)
    gdf = gpd.read_file(shp)
    gdf["tract_fips"] = gdf["STATEFP"] + gdf["COUNTYFP"] + gdf["TRACTCE"]
    return gdf[["tract_fips", "geometry", "STATEFP", "COUNTYFP"]]


def state_fips_from_abbrev(abbrev: str) -> str:
    if abbrev not in STATE_FIPS:
        raise ValueError(f"Unknown state abbreviation: {abbrev}")
    return STATE_FIPS[abbrev]
