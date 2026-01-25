from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import pandas as pd


def normalize_year_column(df: pd.DataFrame) -> pd.DataFrame:
    if "year" not in df.columns:
        return df
    out = df.copy()

    def _to_date(val: Any) -> Optional[datetime]:
        if pd.isna(val):
            return None
        try:
            return datetime(int(val), 1, 1)
        except (TypeError, ValueError):
            return None

    out["year"] = out["year"].apply(_to_date)
    return out
