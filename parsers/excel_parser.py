from __future__ import annotations

from datetime import datetime
from typing import List

import pandas as pd

from analytics.stats import build_shift_records_from_rows, ShiftParseConfig


EXPECTED_COLUMNS = {
    "employee_id": "employee_id",
    "date": "date",
    "start_time": "start_time",
    "end_time": "end_time",
    "status": "raw_status",
}


class ExcelShiftParser:
    """Excel からシフトを読み込むシンプルな実装。"""

    def __init__(self, config: ShiftParseConfig | None = None) -> None:
        self.config = config or ShiftParseConfig()

    def read(self, file) -> pd.DataFrame:
        df = pd.read_excel(file)
        df = self._normalize_columns(df)
        records = build_shift_records_from_rows(df.to_dict(orient="records"), self.config)
        return pd.DataFrame([r.to_dict() for r in records])

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        lower_map = {col.lower(): col for col in df.columns}
        rename_map = {}
        for key, target in EXPECTED_COLUMNS.items():
            if key in lower_map:
                rename_map[lower_map[key]] = target
        normalized = df.rename(columns=rename_map)
        # 日付を date 型に揃える
        if "date" in normalized.columns:
            normalized["date"] = normalized["date"].apply(self._parse_date)
        return normalized[[col for col in EXPECTED_COLUMNS.values() if col in normalized.columns]]

    @staticmethod
    def _parse_date(value):
        if pd.isna(value):
            return None
        if isinstance(value, datetime):
            return value.date()
        try:
            return pd.to_datetime(value).date()
        except Exception:
            return None


__all__ = ["ExcelShiftParser"]
