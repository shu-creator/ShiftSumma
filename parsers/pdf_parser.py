from __future__ import annotations

import re
from calendar import monthrange
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Sequence, Tuple

import pdfplumber
import pandas as pd

from analytics.stats import ShiftParseConfig, build_shift_records_from_rows

TIME_PATTERN = re.compile(r"(\d{1,2}:\d{2})")
STATUS_PATTERN = re.compile(r"(非番|公休|休|／|ー)")
EMPLOYEE_ID_PATTERN = re.compile(r"^\d{6}$")
DAY_PATTERN = re.compile(r"^(?:[1-9]|[12]\d|3[01])$")

LINE_TOLERANCE = 6.0
COLUMN_MERGE_TOLERANCE = 1.5


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
        day_columns = self._detect_day_columns(words)
        if not day_columns:
            return []

        anchors = self._find_employee_anchors(words)
        parsed_rows: List[Dict] = []

        for idx, anchor in enumerate(anchors):
            band_top = anchor["top"]
            band_bottom = anchors[idx + 1]["top"] if idx + 1 < len(anchors) else page.height
            band_tokens = self._slice_tokens(words, band_top, band_bottom)

            entry_y, exit_y = self._resolve_entry_exit_lines(band_tokens)
            if entry_y is None and exit_y is None:
                continue

            start_map, end_map = self._collect_entry_exit_maps(
                band_tokens, day_columns, entry_y, exit_y
            )

            parsed_rows.extend(
                self._build_shift_rows(
                    employee_id=anchor["employee_id"],
                    target_month=target_month,
                    day_columns=day_columns,
                    start_map=start_map,
                    end_map=end_map,
                )
            )

        return parsed_rows

    def _detect_day_columns(self, words: Sequence[dict]) -> List[float]:
        """曜日や日付数字の x 座標を列として抽出する。"""

        x_positions: List[float] = []
        for word in words:
            text = word.get("text", "")
            if text in ["月", "火", "水", "木", "金", "土", "日"] or DAY_PATTERN.fullmatch(text):
                x_center = (word["x0"] + word["x1"]) / 2
                x_positions.append(round(x_center, 2))

        x_positions.sort()

        # 近い座標は同一列としてマージして日付順に並べる
        merged: List[float] = []
        for pos in x_positions:
            if not merged or abs(pos - merged[-1]) > COLUMN_MERGE_TOLERANCE:
                merged.append(pos)
        return merged

    def _find_employee_anchors(self, words: List[dict]) -> List[Dict]:
        """6桁IDのみを社員行として抽出する。"""

        anchors: List[Dict] = []
        for word in words:
            text = word.get("text", "").strip()
            if EMPLOYEE_ID_PATTERN.fullmatch(text) and not text.startswith("0"):
                anchors.append(
                    {
                        "employee_id": text,
                        "top": word.get("top", 0.0),
                        "bottom": word.get("bottom", 0.0),
                    }
                )
        anchors.sort(key=lambda x: x["top"])
        return anchors

    def _slice_tokens(self, words: List[dict], band_top: float, band_bottom: float) -> List[dict]:
        """指定領域のトークンだけを抽出する。"""

        filtered = []
        for word in words:
            y_center = (word.get("top", 0.0) + word.get("bottom", 0.0)) / 2
            if band_top <= y_center <= band_bottom:
                filtered.append(word)
        return filtered

    def _find_line_y(self, tokens: List[dict], keyword: str) -> float | None:
        """キーワード(入/退)の y 座標を返す。"""

        candidates = [
            (token.get("top", 0.0) + token.get("bottom", 0.0)) / 2
            for token in tokens
            if token.get("text") == keyword
        ]
        if not candidates:
            return None
        return sum(candidates) / len(candidates)

    def _collect_line_values(
        self, tokens: Sequence[dict], line_y: float, day_columns: List[float]
    ) -> Dict[int, Dict[str, str]]:
        """行上の時刻/ステータスを日付にスナップする。"""

        values: Dict[int, Dict[str, str]] = defaultdict(dict)
        for token in tokens:
            y_center = (token.get("top", 0.0) + token.get("bottom", 0.0)) / 2
            if abs(y_center - line_y) > LINE_TOLERANCE:
                continue
            text = token.get("text", "").strip()
            time_match = TIME_PATTERN.search(text)
            status_match = STATUS_PATTERN.search(text)
            if not time_match and not status_match:
                continue

            day = self._nearest_day((token.get("x0", 0.0) + token.get("x1", 0.0)) / 2, day_columns)
            if day is None:
                continue

            if time_match and "time" not in values[day]:
                values[day]["time"] = time_match.group(1)
            if status_match and "status" not in values[day]:
                values[day]["status"] = status_match.group(1)
        return values

    def _guess_time_lines(self, tokens: List[dict]) -> List[float]:
        """"入"/"退"が見つからない場合のフォールバック行推定。"""

        line_buckets: Dict[float, int] = defaultdict(int)
        for token in tokens:
            text = token.get("text", "").strip()
            if not (TIME_PATTERN.search(text) or STATUS_PATTERN.search(text)):
                continue
            y_center = (token.get("top", 0.0) + token.get("bottom", 0.0)) / 2
            key = round(y_center, 1)
            line_buckets[key] += 1

        if not line_buckets:
            return []

        sorted_lines = [k for k, _ in sorted(line_buckets.items(), key=lambda x: x[0])]
        return sorted_lines[:2]

    def _collect_entry_exit_maps(
        self,
        tokens: Sequence[dict],
        day_columns: List[float],
        entry_y: float | None,
        exit_y: float | None,
    ) -> Tuple[Dict[int, Dict[str, str]], Dict[int, Dict[str, str]]]:
        """入退行を決定してマッピングする。"""

        if entry_y is None and exit_y is None:
            return {}, {}

        if entry_y is None:
            entry_y = exit_y
        if exit_y is None:
            exit_y = entry_y

        start_map = self._collect_line_values(tokens, entry_y, day_columns)
        end_map = self._collect_line_values(tokens, exit_y, day_columns)
        return start_map, end_map

    def _resolve_entry_exit_lines(self, tokens: Sequence[dict]) -> Tuple[float | None, float | None]:
        """入退刻行の候補を決定する。"""

        entry_y = self._find_line_y(tokens, "入")
        exit_y = self._find_line_y(tokens, "退")

        if entry_y is None or exit_y is None:
            time_lines = self._guess_time_lines(tokens)
            if entry_y is None and time_lines:
                entry_y = time_lines[0]
            if exit_y is None and len(time_lines) > 1:
                exit_y = time_lines[1]
            elif exit_y is None and entry_y is not None:
                exit_y = entry_y
        return entry_y, exit_y

    def _build_shift_rows(
        self,
        employee_id: str,
        target_month: str,
        day_columns: List[float],
        start_map: Dict[int, Dict[str, str]],
        end_map: Dict[int, Dict[str, str]],
    ) -> List[Dict]:
        """start/ end のマップから日別の行を組み立てる。"""

        rows: List[Dict] = []
        max_days = self._days_in_month(target_month)
        for day in range(1, max_days + 1):
            start_info = start_map.get(day, {})
            end_info = end_map.get(day, {})

            start_time = start_info.get("time")
            end_time = end_info.get("time")
            raw_status = start_info.get("status") or end_info.get("status")

            # 何も情報が無い日はスキップ
            if not any([start_time, end_time, raw_status]):
                continue

            try:
                date_str = f"{target_month}-{day:02d}"
                work_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            rows.append(
                {
                    "employee_id": employee_id,
                    "date": work_date,
                    "start_time": start_time,
                    "end_time": end_time,
                    "raw_status": raw_status,
                }
            )
        return rows

    @staticmethod
    def _nearest_day(x_center: float, columns: List[float]) -> int | None:
        if not columns:
            return None
        nearest = min(columns, key=lambda c: abs(c - x_center))
        return columns.index(nearest) + 1

    @staticmethod
    def _days_in_month(target_month: str) -> int:
        try:
            year, month = target_month.split("-")
            return monthrange(int(year), int(month))[1]
        except Exception:
            return 31


__all__ = ["PdfShiftParser"]
