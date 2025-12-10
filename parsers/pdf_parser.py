from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

import pdfplumber
import pandas as pd

from analytics.stats import ShiftParseConfig, build_shift_records_from_rows

TIME_PATTERN = re.compile(r"\b(\d{1,2}:\d{2})\b")
STATUS_PATTERN = re.compile(r"(非番|公休|休)")


class PdfShiftParser:
    """pdfplumber を使った座標ベースのたたき台実装。"""

    def __init__(self, config: ShiftParseConfig | None = None) -> None:
        self.config = config or ShiftParseConfig()

    def read(self, file, target_month: str) -> pd.DataFrame:
        # target_month: "YYYY-MM"
        rows: List[Dict] = []
        with pdfplumber.open(file) as pdf:
            for page in pdf.pages:
                rows.extend(self._extract_page(page, target_month))
        records = build_shift_records_from_rows(rows, self.config)
        return pd.DataFrame([r.to_dict() for r in records])

    def _extract_page(self, page, target_month: str) -> List[Dict]:
        words = page.extract_words(use_text_flow=True, keep_blank_chars=False)
        columns = self._detect_day_columns(words)
        employee_rows = self._group_by_employee(words)

        parsed_rows: List[Dict] = []
        for employee_id, tokens in employee_rows.items():
            parsed_rows.extend(
                self._extract_rows_for_employee(employee_id, tokens, columns, target_month)
            )
        return parsed_rows

    def _detect_day_columns(self, words):
        # 曜日行から x0 を抽出し、最寄りの列を求める。
        day_columns = {}
        for word in words:
            if word.get("text") in ["月", "火", "水", "木", "金", "土", "日"]:
                day_columns[round(word["x0"], 1)] = word.get("text")
        return sorted(day_columns.keys())

    def _group_by_employee(self, words):
        employee_rows: Dict[str, List[dict]] = defaultdict(list)
        current_emp = None
        for word in words:
            text = word.get("text", "")
            if text.isdigit():
                current_emp = text
            if current_emp:
                employee_rows[current_emp].append(word)
        return employee_rows

    def _extract_rows_for_employee(self, employee_id: str, tokens: List[dict], columns, target_month: str):
        rows = []
        for token in tokens:
            text = token.get("text", "")
            match_time = TIME_PATTERN.findall(text)
            if not match_time and not STATUS_PATTERN.search(text):
                continue
            day = self._nearest_day(token.get("x0"), columns)
            if day is None:
                continue
            # 日付を target_month + day で構築
            try:
                date_str = f"{target_month}-{int(day):02d}"
                work_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            row: Dict = {"employee_id": employee_id, "date": work_date}
            if len(match_time) >= 2:
                row["start_time"], row["end_time"] = match_time[:2]
            elif len(match_time) == 1:
                row["start_time"] = match_time[0]
            status_match = STATUS_PATTERN.search(text)
            if status_match:
                row["raw_status"] = status_match.group(1)
            rows.append(row)
        return rows

    @staticmethod
    def _nearest_day(x0: float, columns) -> int | None:
        if not columns:
            return None
        nearest = min(columns, key=lambda c: abs(c - x0))
        # 列リストを 1..len で日付にマッピング
        return columns.index(nearest) + 1


__all__ = ["PdfShiftParser"]
