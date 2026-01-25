from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import requests

from wesdash.datasets.acs_common import census_get, dataset_exists, variables_index_optional
from wesdash.geo import crosswalks
from wesdash.geo import tiger
from wesdash.io.cache import raw_dir

B01001_VARS = [
    "B01001_001E",
    "B01001_003E",
    "B01001_004E",
    "B01001_005E",
    "B01001_027E",
    "B01001_028E",
    "B01001_029E",
]
S1101_VAR = "S1101_C01_005E"
B14003_VARS = [
    "B14003_004E",
    "B14003_005E",
    "B14003_006E",
    "B14003_032E",
    "B14003_033E",
    "B14003_034E",
    "B14003_013E",
    "B14003_014E",
    "B14003_015E",
    "B14003_041E",
    "B14003_042E",
    "B14003_043E",
]
B19131_GROUP = "B19131"
logger = logging.getLogger(__name__)


def _census_get_county(
    year: int,
    dataset: str,
    variables: List[str],
    state_fips: str,
    county_code: str,
    api_key: Optional[str],
) -> Optional[List[List[str]]]:
    try:
        data = census_get(
            year,
            dataset,
            variables,
            f"county:{county_code}",
            in_clause=f"state:{state_fips}",
            api_key=api_key,
        )
        if len(data) < 2:
            return None
        return data
    except requests.RequestException as exc:
        status = exc.response.status_code if exc.response is not None else None
        detail = exc.response.text.strip() if exc.response is not None else str(exc)
        if status in (400, 404):
            logger.warning(
                "Skipping %s %s state:%s county:%s: %s",
                dataset,
                year,
                state_fips,
                county_code,
                detail,
            )
            return None
        logger.warning(
            "Skipping %s %s state:%s county:%s due to request error: %s",
            dataset,
            year,
            state_fips,
            county_code,
            detail,
        )
        return None


def _select_income_vars(meta: Dict[str, Any], income_labels: List[str]) -> List[str]:
    variables = meta.get("variables", {})
    selected: List[str] = []
    for var, info in variables.items():
        if not var.endswith("E"):
            continue
        label = info.get("label", "")
        if "With own children of the householder under 18 years" not in label:
            continue
        if any(il in label for il in income_labels):
            selected.append(var)
    return sorted(set(selected))


def _discover_income_vars(year: int) -> Dict[str, List[str]]:
    meta = variables_index_optional(year, "acs1", group=B19131_GROUP)
    if meta is None:
        return {"income_150": [], "income_200": []}
    v150 = _select_income_vars(meta, ["$150,000 to $199,999"])
    v200 = _select_income_vars(meta, ["$200,000 or more"])
    return {"income_150": v150, "income_200": v200}


def _available_years(start_year: int, end_year: int) -> List[int]:
    years: List[int] = []
    for y in range(start_year, end_year + 1):
        if dataset_exists(y, "acs1"):
            years.append(y)
    return years


def _target_counties(cfg: Dict[str, Any], target_zctas: List[str]) -> Any:
    cache_dir = cfg["paths"]["geo_cache_dir"]
    zcta_gdf = tiger.load_zcta(cache_dir)
    zcta_gdf = zcta_gdf[zcta_gdf["zcta5"].isin(target_zctas)]
    counties = tiger.load_counties(cache_dir, [tiger.STATE_FIPS["DC"], tiger.STATE_FIPS["MD"]])
    if zcta_gdf.empty:
        return counties
    try:
        weights = crosswalks.county_zcta_area_weights(zcta_gdf, counties)
    except Exception as exc:
        logger.warning("Falling back to all counties for ACS1 fetch: %s", exc)
        return counties
    county_fips = sorted(weights["county_fips"].unique())
    return counties[counties["county_fips"].isin(county_fips)].copy()


def fetch(cfg: Dict[str, Any]) -> List[str]:
    start_year = cfg["project"]["start_year"]
    current_year = cfg["project"].get("current_year")
    if not current_year:
        from datetime import datetime

        current_year = datetime.utcnow().year

    years = _available_years(start_year, current_year)
    api_key = None
    api_env = cfg["datasets"].get("acs", {}).get("api_key_env", "CENSUS_API_KEY")
    api_key = cfg.get("env", {}).get(api_env)
    if api_key is None:
        api_key = None

    target_zctas = cfg["geography"].get("target_zctas") or cfg["geography"]["target_zips"]
    target_zctas = [str(z).zfill(5) for z in target_zctas]
    counties = _target_counties(cfg, target_zctas)
    counties = counties[["county_fips"]].copy()
    counties["state_fips"] = counties["county_fips"].str[:2]
    counties["county_code"] = counties["county_fips"].str[2:]

    raw_base = raw_dir(cfg, "acs_1y_allocated")
    out_files: List[str] = []
    for year in years:
        income_vars = _discover_income_vars(year)
        income_all = income_vars["income_150"] + income_vars["income_200"]
        base_vars = B01001_VARS + B14003_VARS + income_all
        header: List[str] = []
        rows: List[List[str]] = []
        for _, row in counties.iterrows():
            base_data = _census_get_county(
                year,
                "acs1",
                base_vars,
                row["state_fips"],
                row["county_code"],
                api_key,
            )
            if base_data is None:
                continue
            base_header = base_data[0]
            base_row = base_data[1]
            subject_data = _census_get_county(
                year,
                "acs1/subject",
                [S1101_VAR],
                row["state_fips"],
                row["county_code"],
                api_key,
            )
            subject_value = None
            if subject_data is not None:
                subject_header = subject_data[0]
                subject_row = subject_data[1]
                subject_idx = subject_header.index(S1101_VAR)
                subject_value = subject_row[subject_idx]

            if not header:
                header = base_header + [S1101_VAR]
            combined = base_row + [subject_value]
            rows.append(combined)
        if not rows:
            logger.warning("No ACS1 rows produced for %s; skipping year.", year)
            continue
        payload = {"year": year, "header": header, "rows": rows, "income_vars": income_vars}
        out_path = f"{raw_base}/acs1_{year}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        out_files.append(out_path)
    return out_files
