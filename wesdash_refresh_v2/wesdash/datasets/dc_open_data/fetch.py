from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

import requests

from wesdash.io.cache import raw_dir


logger = logging.getLogger(__name__)


def fetch(cfg: Dict[str, Any]) -> List[str]:
    ds_cfg = cfg["datasets"].get("dc_open_data", {})
    domain = ds_cfg.get("domain", "data.dc.gov")
    datasets = ds_cfg.get("datasets", [])
    if not datasets:
        raise ValueError("datasets.dc_open_data.datasets must be configured")

    valid_datasets: List[Dict[str, Any]] = []
    for ds in datasets:
        dataset_id = ds.get("dataset_id")
        if not dataset_id or dataset_id == "REPLACE_ME":
            logger.warning("Skipping dc_open_data dataset with missing dataset_id (name=%s)", ds.get("name"))
            continue
        valid_datasets.append(ds)
    if not valid_datasets:
        raise ValueError("datasets.dc_open_data.datasets must include a valid dataset_id")

    app_token = ds_cfg.get("app_token")
    headers = {"X-App-Token": app_token} if app_token else {}

    out_files: List[str] = []
    base = raw_dir(cfg, "dc_open_data")
    for ds in valid_datasets:
        dataset_id = ds["dataset_id"]
        name = ds.get("name", dataset_id)
        soql = ds.get("soql")
        limit = ds.get("limit", 50000)
        offset = 0
        rows: List[Dict[str, Any]] = []
        try:
            while True:
                params = {"$limit": limit, "$offset": offset}
                if soql:
                    params["$query"] = soql
                url = f"https://{domain}/resource/{dataset_id}.json"
                r = requests.get(url, params=params, headers=headers, timeout=60)
                r.raise_for_status()
                batch = r.json()
                if not batch:
                    break
                rows.extend(batch)
                offset += limit
        except requests.exceptions.RequestException as exc:
            logger.warning("Skipping dc_open_data dataset %s due to request error: %s", name, exc)
            continue
        if not rows:
            logger.warning("Skipping dc_open_data dataset %s due to empty response", name)
            continue
        out_path = f"{base}/{name}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(rows, f)
        out_files.append(out_path)
    if not out_files:
        raise ValueError("dc_open_data produced no files; check configuration or connectivity")
    return out_files
