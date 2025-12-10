from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
from typing import Any, Dict, List, Optional


@dataclass
class ShiftRecord:
    """社員×日単位のシフト基本モデル。"""

    employee_id: str
    date: date
    weekday: str
    week_index: int
    start_time: Optional[str]
    end_time: Optional[str]
    minutes: int
    slot: str
    is_half: bool
    is_weekday: bool
    raw_status: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WeeklyEmployeeStats:
    employee_id: str
    week_index: int
    week_start_date: date
    week_minutes: int
    week_hours: float
    week_workdays: int
    week_half_days: int
    week_half_ratio: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WeeklyTeamStats:
    week_index: int
    week_start_date: date
    total_minutes: int
    total_hours: float
    avg_hours_per_employee: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WeekdaySlotStats:
    weekday: str
    slot: str
    count: int
    ratio_in_day: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ShiftParseConfig:
    """実働判定に用いる閾値設定。"""

    full_threshold_minutes: int = 270  # 4.5h
    half_min_minutes: int = 180  # 3h

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def records_to_dicts(records: List[ShiftRecord]) -> List[Dict[str, Any]]:
    return [record.to_dict() for record in records]
