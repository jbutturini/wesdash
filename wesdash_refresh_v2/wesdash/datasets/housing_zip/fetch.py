from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import logging

import requests
import shutil

from wesdash.io.cache import raw_dir
from wesdash.io.http import download_file

logger = logging.getLogger(__name__)


def _as_url_list(val: Any) -> List[str]:
    if isinstance(val, list):
        return [str(v) for v in val if v]
    if isinstance(val, str) and val:
        return [val]
    return []


def _candidate_sources(val: Any) -> List[Tuple[str, str]]:
    if isinstance(val, dict):
        local_path = val.get("local_path")
        if local_path:
            return [("local", str(local_path))]
        urls = val.get("urls") or val.get("url") or []
        return [("url", u) for u in _as_url_list(urls)]
    if isinstance(val, str):
        if Path(val).exists():
            return [("local", val)]
        return [("url", val)] if val else []
    if isinstance(val, list):
        return [("url", u) for u in _as_url_list(val)]
    return []


def fetch(cfg: Dict[str, Any]) -> List[str]:
    ds_cfg = cfg["datasets"].get("housing_zip", {})
    files = ds_cfg.get("files", {})
    if not files:
        raise ValueError("datasets.housing_zip.files must be configured")

    out_files: List[str] = []
    base = raw_dir(cfg, "housing_zip")
    for metric, url in files.items():
        out_path = f"{base}/{metric}.csv"
        candidates = _candidate_sources(url)
        if not candidates:
            raise ValueError(f"housing_zip.files.{metric} must be a URL, list of URLs, or local_path")
        downloaded = False
        for kind, candidate in candidates:
            if kind == "local":
                if Path(candidate).exists():
                    shutil.copyfile(candidate, out_path)
                    out_files.append(out_path)
                    downloaded = True
                    break
                logger.warning("housing_zip %s local file not found: %s", metric, candidate)
                continue
            try:
                download_file(candidate, out_path)
                out_files.append(out_path)
                downloaded = True
                break
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    logger.warning("housing_zip %s URL not found: %s", metric, candidate)
                    continue
                raise
        if not downloaded:
            raise RuntimeError(f"housing_zip {metric} download failed for all URLs")
    return out_files
