from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

import requests

from wesdash.datasets.acs_common import census_get, dataset_exists, variables_index_optional
from wesdash.geo.zcta import zcta_state_map
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


def _census_get_zcta(
    year: int,
    dataset: str,
    variables: List[str],
    zcta: str,
    state_fips: Optional[str],
    api_key: Optional[str],
) -> Optional[List[List[str]]]:
    primary_in = f"state:{state_fips}" if (year <= 2018 and state_fips) else None
    fallback_in = None
    if primary_in is None and state_fips:
        fallback_in = f"state:{state_fips}"
    attempts = []
    if primary_in not in attempts:
        attempts.append(primary_in)
    if fallback_in not in attempts:
        attempts.append(fallback_in)

    last_exc: Optional[requests.HTTPError] = None
    for in_clause in attempts:
        try:
            data = census_get(
                year,
                dataset,
                variables,
                f"zip code tabulation area:{str(zcta).zfill(5)}",
                in_clause=in_clause,
                api_key=api_key,
            )
            if len(data) < 2:
                logger.warning("Empty response for ZCTA %s %s %s", zcta, dataset, year)
                return None
            return data
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 400:
                last_exc = exc
                continue
            raise
    if last_exc is not None:
        detail = last_exc.response.text.strip() if last_exc.response is not None else str(last_exc)
        logger.warning("Skipping ZCTA %s for %s %s: %s", zcta, dataset, year, detail)
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
    meta = variables_index_optional(year, "acs5", group=B19131_GROUP)
    if meta is None:
        return {"income_150": [], "income_200": []}
    v150 = _select_income_vars(meta, ["$150,000 to $199,999"])
    v200 = _select_income_vars(meta, ["$200,000 or more"])
    return {"income_150": v150, "income_200": v200}


def _available_years(start_year: int, end_year: int) -> List[int]:
    years: List[int] = []
    for y in range(start_year, end_year + 1):
        if dataset_exists(y, "acs5"):
            years.append(y)
    return years


def fetch(cfg: Dict[str, Any]) -> List[str]:
    target_zctas = cfg["geography"].get("target_zctas") or cfg["geography"]["target_zips"]
    target_zctas = [str(z).zfill(5) for z in target_zctas]
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

    out_files: List[str] = []
    state_map = zcta_state_map(cfg["paths"]["geo_cache_dir"], target_zctas)
    raw_base = raw_dir(cfg, "acs_5y")
    for year in years:
        income_vars = _discover_income_vars(year)
        income_all = income_vars["income_150"] + income_vars["income_200"]
        base_vars = B01001_VARS + B14003_VARS
        rows: List[List[str]] = []
        header: List[str] = []
        for zcta in target_zctas:
            state_fips = state_map.get(str(zcta).zfill(5))
            base_data = _census_get_zcta(year, "acs5", base_vars, zcta, state_fips, api_key)
            if base_data is None:
                continue
            base_header = base_data[0]
            base_row = base_data[1]

            subject_data = _census_get_zcta(year, "acs5/subject", [S1101_VAR], zcta, state_fips, api_key)
            subject_value = None
            if subject_data is not None:
                subject_header = subject_data[0]
                subject_row = subject_data[1]
                subject_idx = subject_header.index(S1101_VAR)
                subject_value = subject_row[subject_idx]

            income_values: List[Optional[str]] = []
            if income_all:
                income_data = _census_get_zcta(year, "acs5", income_all, zcta, state_fips, api_key)
                if income_data is not None:
                    income_header = income_data[0]
                    income_row = income_data[1]
                    idx_map = {col: i for i, col in enumerate(income_header)}
                    for var in income_all:
                        income_values.append(income_row[idx_map[var]])
                else:
                    income_values = [None] * len(income_all)

            if not header:
                header = base_header + [S1101_VAR] + income_all
            combined = base_row + [subject_value] + income_values
            rows.append(combined)

        if not rows:
            logger.warning("No ACS5 rows produced for %s; skipping year.", year)
            continue
        payload = {"year": year, "header": header, "rows": rows, "income_vars": income_vars}
        out_path = f"{raw_base}/acs5_{year}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        out_files.append(out_path)
    return out_files
