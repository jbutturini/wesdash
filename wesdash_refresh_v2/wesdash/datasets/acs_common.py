from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import requests

from wesdash.io.http import build_session

def census_base_url(year: int, dataset: str) -> str:
    return f"https://api.census.gov/data/{year}/acs/{dataset}"


def dataset_exists(year: int, dataset: str) -> bool:
    url = f"{census_base_url(year, dataset)}/variables.json"
    try:
        r = requests.get(url, timeout=20)
        return r.status_code == 200
    except requests.RequestException:
        return False


def variables_index(year: int, dataset: str, group: Optional[str] = None) -> Dict[str, Any]:
    if group:
        url = f"{census_base_url(year, dataset)}/groups/{group}.json"
    else:
        url = f"{census_base_url(year, dataset)}/variables.json"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def variables_index_optional(year: int, dataset: str, group: Optional[str]) -> Optional[Dict[str, Any]]:
    try:
        return variables_index(year, dataset, group)
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return None
        raise


def census_get(
    year: int,
    dataset: str,
    variables: List[str],
    for_clause: str,
    in_clause: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[List[str]]:
    url = census_base_url(year, dataset)
    params = {"get": ",".join(["NAME"] + variables), "for": for_clause}
    if in_clause:
        params["in"] = in_clause
    if api_key:
        params["key"] = api_key
    session = build_session()
    headers = {"Accept": "application/json"}
    backoff = 0.5
    for attempt in range(3):
        r = session.get(url, params=params, headers=headers, timeout=60)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            if attempt < 2 and r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
        text = r.text.strip()
        if not text:
            if attempt < 2:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise requests.RequestException("Empty response from Census API", response=r)
        try:
            return r.json()
        except ValueError:
            if attempt < 2:
                time.sleep(backoff)
                backoff *= 2
                continue
            snippet = text[:200]
            raise requests.RequestException(
                f"Invalid JSON response from Census API: {snippet}",
                response=r,
            )
    raise requests.RequestException("Census API request failed after retries")
