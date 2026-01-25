import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv


def load_config(path: str) -> Dict[str, Any]:
    load_dotenv()
    cfg_path = Path(path).resolve()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    base_dir = cfg_path.parent.parent
    cfg.setdefault("project", {})
    cfg.setdefault("geography", {})
    cfg.setdefault("datasets", {})

    cfg["project"].setdefault("start_year", 2015)
    cfg["project"].setdefault("output_excel", "output/wes_board_dashboard.xlsx")
    cfg["project"].setdefault("raw_dir", "data/raw")
    cfg["project"].setdefault("processed_dir", "data/processed")
    cfg["project"].setdefault("geo_cache_dir", "data/geo")

    target_zips = cfg["geography"].get("target_zips")
    if not target_zips:
        raise ValueError("Config must include geography.target_zips")

    cfg["geography"].setdefault("zip_to_zcta_overrides", {})
    cfg["geography"].setdefault("target_zctas", [])

    cfg["paths"] = {
        "base_dir": str(base_dir),
        "raw_dir": str((base_dir / cfg["project"]["raw_dir"]).resolve()),
        "processed_dir": str((base_dir / cfg["project"]["processed_dir"]).resolve()),
        "geo_cache_dir": str((base_dir / cfg["project"]["geo_cache_dir"]).resolve()),
        "output_excel": str((base_dir / cfg["project"]["output_excel"]).resolve()),
    }

    return cfg


def get_env_key(cfg: Dict[str, Any], key: str) -> str:
    val = os.getenv(key, "")
    if not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val
