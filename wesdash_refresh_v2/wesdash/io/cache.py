from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd


def today_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def ensure_dir(path: str) -> str:
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def raw_dir(cfg: dict, dataset: str, date_str: Optional[str] = None) -> str:
    date_part = date_str or today_str()
    base = Path(cfg["paths"]["raw_dir"]) / dataset / date_part
    return ensure_dir(str(base))


def processed_dir(cfg: dict, dataset: str, date_str: Optional[str] = None) -> str:
    date_part = date_str or today_str()
    base = Path(cfg["paths"]["processed_dir"]) / dataset / date_part
    return ensure_dir(str(base))


def latest_processed_dir(cfg: dict, dataset: str) -> Optional[str]:
    base = Path(cfg["paths"]["processed_dir"]) / dataset
    if not base.exists():
        return None
    dates = sorted([p for p in base.iterdir() if p.is_dir()])
    if not dates:
        return None
    return str(dates[-1])


def write_parquet(df: pd.DataFrame, path: str) -> str:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path
