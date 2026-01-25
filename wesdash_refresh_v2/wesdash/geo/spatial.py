from __future__ import annotations

from typing import Iterable

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


def points_to_zcta(
    df: pd.DataFrame,
    lat_col: str,
    lon_col: str,
    zcta_gdf: gpd.GeoDataFrame,
) -> pd.DataFrame:
    points = df[[lat_col, lon_col]].copy()
    points = points.dropna(subset=[lat_col, lon_col])
    geometry = [Point(xy) for xy in zip(points[lon_col], points[lat_col])]
    gdf = gpd.GeoDataFrame(points, geometry=geometry, crs="EPSG:4326")
    zcta = zcta_gdf.to_crs("EPSG:4326")
    joined = gpd.sjoin(gdf, zcta, how="left", predicate="intersects")
    df = df.copy()
    df.loc[points.index, "zcta5"] = joined["zcta5"].values
    df["zcta5"] = df["zcta5"].astype(str).str.zfill(5)
    return df


def clip_geos(gdf: gpd.GeoDataFrame, keep_values: Iterable[str], column: str) -> gpd.GeoDataFrame:
    keep_set = set(keep_values)
    return gdf[gdf[column].isin(keep_set)].copy()
