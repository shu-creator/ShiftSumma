from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set

import pdfplumber
import pandas as pd

from analytics.stats import ShiftParseConfig, build_shift_records_from_rows

TIME_PATTERN = re.compile(r"\b(\d{1,2}:\d{2})\b")
STATUS_PATTERN = re.compile(r"(非番|公休|休)")
EMPLOYEE_ID_PATTERN = re.compile(r"^\d{6,}$")


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
            text = (word.get("text") or "").strip()
            if not text:
                continue
            if EMPLOYEE_ID_PATTERN.match(text):
                current_emp = text
            if current_emp:
                employee_rows[current_emp].append(word)
        return employee_rows

    def _extract_rows_for_employee(self, employee_id: str, tokens: List[dict], columns, target_month: str):
        if not tokens:
            return []

        start_times: Dict[int, str] = {}
        end_times: Dict[int, str] = {}
        status_by_day: Dict[int, str] = {}
        days_with_tokens: Set[int] = set()

        current_section = None
        for token in tokens:
            text = (token.get("text") or "").strip()
            if not text:
                continue
            if text == "入":
                current_section = "start"
                continue
            if text == "退":
                current_section = "end"
                continue
            if current_section not in {"start", "end"}:
                continue

            day = self._nearest_day(token.get("x0"), columns)
            if day is None:
                continue

            days_with_tokens.add(day)

            times = TIME_PATTERN.findall(text)
            if times:
                if current_section == "start" and day not in start_times:
                    start_times[day] = times[0]
                if current_section == "end" and day not in end_times:
                    end_times[day] = times[0]

            status_match = STATUS_PATTERN.search(text)
            if status_match and day not in status_by_day:
                status_by_day[day] = status_match.group(1)

        rows: List[Dict] = []
        days_to_emit = sorted(days_with_tokens | set(start_times.keys()) | set(end_times.keys()) | set(status_by_day.keys()))

        for day in days_to_emit:
            try:
                date_str = f"{target_month}-{int(day):02d}"
                work_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            row: Dict = {"employee_id": employee_id, "date": work_date}
            if day in start_times:
                row["start_time"] = start_times[day]
            if day in end_times:
                row["end_time"] = end_times[day]
            if day in status_by_day:
                row["raw_status"] = status_by_day[day]
            rows.append(row)

        self._fix_misaligned_end_times(rows)

        return rows

    def _fix_misaligned_end_times(self, rows: List[Dict]) -> None:
        if not rows:
            return
        for idx in range(len(rows) - 1):
            curr = rows[idx]
            nxt = rows[idx + 1]
            if curr.get("raw_status") or nxt.get("raw_status"):
                continue
            if not curr.get("start_time"):
                continue
            curr_date = curr.get("date")
            next_date = nxt.get("date")
            if not curr_date or not next_date:
                continue
            if (next_date - curr_date).days != 1:
                continue
            curr_duration = self._duration_minutes(curr.get("start_time"), curr.get("end_time"))
            next_duration = self._duration_minutes(nxt.get("start_time"), nxt.get("end_time"))
            if curr_duration is None or next_duration is None:
                continue
            if curr_duration >= self.config.full_threshold_minutes:
                continue
            if next_duration < self.config.full_threshold_minutes:
                continue
            if curr.get("start_time") != nxt.get("start_time"):
                continue
            curr["end_time"] = nxt["end_time"]

    @staticmethod
    def _duration_minutes(start_time: str | None, end_time: str | None) -> int | None:
        if not start_time or not end_time:
            return None
        try:
            sh, sm = start_time.split(":")
            eh, em = end_time.split(":")
            start_minutes = int(sh) * 60 + int(sm)
            end_minutes = int(eh) * 60 + int(em)
        except (ValueError, AttributeError):
            return None
        duration = end_minutes - start_minutes
        if duration < 0:
            duration += 24 * 60
        return duration

    @staticmethod
    def _nearest_day(x0: float, columns) -> int | None:
        if not columns:
            return None
        nearest = min(columns, key=lambda c: abs(c - x0))
        # 列リストを 1..len で日付にマッピング
        return columns.index(nearest) + 1


__all__ = ["PdfShiftParser"]
