from __future__ import annotations

from typing import Any, Dict, List

from wesdash.io.cache import raw_dir
from wesdash.io.http import download_file


def fetch(cfg: Dict[str, Any]) -> List[str]:
    ds_cfg = cfg["datasets"].get("osse", {})
    source_url = ds_cfg.get("source_url")
    local_path = ds_cfg.get("local_path")

    if not source_url and not local_path:
        raise ValueError("datasets.osse.source_url or local_path must be configured")

    if local_path:
        return [local_path]

    out_path = f"{raw_dir(cfg, 'osse')}/osse.xlsx"
    download_file(source_url, out_path)
    return [out_path]
