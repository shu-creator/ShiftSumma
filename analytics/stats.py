from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable, List, Optional, Sequence

import pandas as pd

from .models import (
    ShiftParseConfig,
    ShiftRecord,
    WeekdaySlotStats,
    WeeklyEmployeeStats,
    WeeklyTeamStats,
)

WEEKDAY_LABELS = ["月", "火", "水", "木", "金", "土", "日"]
HALF_SLOTS = {"AM半日", "PM半日"}
WORKING_SLOTS_ORDER = ["AM半日", "Full", "PM半日"]


def parse_hhmm_to_minutes(value: Optional[str]) -> Optional[int]:
    """"hh:mm"を分に変換。"""

    if value is None:
        return None
    try:
        hour, minute = value.split(":")
        return int(hour) * 60 + int(minute)
    except Exception:
        return None


def compute_week_index(current: date) -> int:
    """月曜始まりで当月内の週番号(1〜)を計算。"""

    first_day = current.replace(day=1)
    first_monday = first_day - timedelta(days=first_day.weekday())
    week_start = current - timedelta(days=current.weekday())
    return ((week_start - first_monday).days // 7) + 1


def determine_slot(
    minutes: int,
    start_time: Optional[str],
    end_time: Optional[str],
    config: ShiftParseConfig,
) -> tuple[str, bool]:
    """実働分からスロットと半日判定を返す。"""

    if minutes <= 0:
        return "NA", False

    if minutes > config.full_threshold_minutes:
        return "Full", False

    if minutes >= config.half_min_minutes:
        # 半日候補
        if end_time and end_time <= "14:30":
            return "AM半日", True
        if start_time and start_time >= "13:30":
            return "PM半日", True
        return "PM半日", True

    # それ以外は暫定で半日扱い
    return "PM半日", True


def build_shift_record(
    employee_id: str,
    work_date: date,
    start_time: Optional[str],
    end_time: Optional[str],
    raw_status: Optional[str],
    config: ShiftParseConfig,
) -> ShiftRecord:
    start_minutes = parse_hhmm_to_minutes(start_time)
    end_minutes = parse_hhmm_to_minutes(end_time)

    minutes = 0
    if start_minutes is not None and end_minutes is not None:
        minutes = end_minutes - start_minutes
        if minutes < 0:
            minutes += 24 * 60

    slot, is_half = determine_slot(minutes, start_time, end_time, config)
    weekday_index = work_date.weekday()
    weekday_label = WEEKDAY_LABELS[weekday_index]

    return ShiftRecord(
        employee_id=str(employee_id),
        date=work_date,
        weekday=weekday_label,
        week_index=compute_week_index(work_date),
        start_time=start_time,
        end_time=end_time,
        minutes=minutes,
        slot=slot,
        is_half=is_half,
        is_weekday=weekday_index < 5,
        raw_status=raw_status,
    )


def build_shift_records_from_rows(
    rows: Iterable[dict],
    config: Optional[ShiftParseConfig] = None,
) -> List[ShiftRecord]:
    """行データからShiftRecordを生成する共通関数。"""

    config = config or ShiftParseConfig()
    records: List[ShiftRecord] = []
    for row in rows:
        work_date = row.get("date")
        if isinstance(work_date, str):
            work_date = datetime.strptime(work_date, "%Y-%m-%d").date()
        if work_date is None:
            continue
        record = build_shift_record(
            employee_id=row.get("employee_id"),
            work_date=work_date,
            start_time=row.get("start_time"),
            end_time=row.get("end_time"),
            raw_status=row.get("raw_status"),
            config=config,
        )
        records.append(record)
    return records


def to_dataframe(records: Sequence[ShiftRecord]) -> pd.DataFrame:
    return pd.DataFrame([record.to_dict() for record in records])


def weekly_employee_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[f.name for f in WeeklyEmployeeStats.__dataclass_fields__.values()])

    grouped = df.groupby(["employee_id", "week_index"])
    agg = grouped.agg(
        week_minutes=("minutes", "sum"),
        week_workdays=("minutes", lambda s: (s > 0).sum()),
        week_half_days=("is_half", "sum"),
        week_start_date=("date", lambda s: (s.min() - timedelta(days=s.min().weekday()))),
    ).reset_index()

    agg["week_hours"] = (agg["week_minutes"] / 60).round(2)
    agg["week_half_ratio"] = agg.apply(
        lambda row: row["week_half_days"] / row["week_workdays"] if row["week_workdays"] else 0.0,
        axis=1,
    )

    return agg[
        [
            "employee_id",
            "week_index",
            "week_start_date",
            "week_minutes",
            "week_hours",
            "week_workdays",
            "week_half_days",
            "week_half_ratio",
        ]
    ].sort_values(["employee_id", "week_index"])


def weekly_team_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[f.name for f in WeeklyTeamStats.__dataclass_fields__.values()])

    agg = df.groupby("week_index").agg(
        total_minutes=("minutes", "sum"),
        total_hours=("minutes", lambda s: (s.sum() / 60).round(2)),
        week_start_date=("date", lambda s: (s.min() - timedelta(days=s.min().weekday()))),
        employee_count=("employee_id", lambda s: s.nunique()),
    ).reset_index()
    agg["avg_hours_per_employee"] = agg.apply(
        lambda row: row["total_hours"] / row["employee_count"] if row["employee_count"] else 0.0,
        axis=1,
    )
    return agg[
        [
            "week_index",
            "week_start_date",
            "total_minutes",
            "total_hours",
            "avg_hours_per_employee",
        ]
    ].sort_values("week_index")


def weekday_slot_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[f.name for f in WeekdaySlotStats.__dataclass_fields__.values()])

    weekday_df = df[df["is_weekday"]].copy()
    if weekday_df.empty:
        return pd.DataFrame(columns=[f.name for f in WeekdaySlotStats.__dataclass_fields__.values()])

    grouped = weekday_df.groupby(["weekday", "slot"]).size().reset_index(name="count")
    total_by_weekday = weekday_df.groupby("weekday").size().rename("total")
    grouped = grouped.merge(total_by_weekday, on="weekday", how="left")
    grouped["ratio_in_day"] = grouped["count"] / grouped["total"]

    return grouped[["weekday", "slot", "count", "ratio_in_day"]].sort_values(["weekday", "slot"])


def weekday_slot_stats_working(df: pd.DataFrame) -> pd.DataFrame:
    """曜日×時間帯（勤務ありのみ）を集計。

    - 対象: minutes > 0 かつ slot in {"AM半日","Full","PM半日"}
    - 表示: 平日（月〜金）のみ
    - ratio_in_day: 同一weekday内（勤務あり）の構成比
    """

    columns = [f.name for f in WeekdaySlotStats.__dataclass_fields__.values()]
    if df.empty:
        return pd.DataFrame(columns=columns)

    working_df = df[(df["minutes"] > 0) & (df["slot"].isin(WORKING_SLOTS_ORDER))].copy()
    working_df = working_df[working_df["is_weekday"]]
    if working_df.empty:
        return pd.DataFrame(columns=columns)

    # 全weekday×slot を作って 0 埋め（表示が安定する）
    full_index = pd.MultiIndex.from_product(
        [WEEKDAY_LABELS[:5], WORKING_SLOTS_ORDER],
        names=["weekday", "slot"],
    )
    counts = working_df.groupby(["weekday", "slot"]).size().reindex(full_index, fill_value=0)
    grouped = counts.reset_index(name="count")
    totals = grouped.groupby("weekday")["count"].transform("sum")
    grouped["ratio_in_day"] = grouped["count"].div(totals.where(totals > 0, 1))
    grouped.loc[totals == 0, "ratio_in_day"] = 0.0

    # 並び順を固定
    grouped["weekday"] = pd.Categorical(grouped["weekday"], categories=WEEKDAY_LABELS[:5], ordered=True)
    grouped["slot"] = pd.Categorical(grouped["slot"], categories=WORKING_SLOTS_ORDER, ordered=True)
    return grouped[["weekday", "slot", "count", "ratio_in_day"]].sort_values(["weekday", "slot"])


def weekday_na_counts(df: pd.DataFrame) -> pd.DataFrame:
    """NA（非勤務）だけの件数を曜日別に集計。

    対象: minutes == 0 かつ 平日(is_weekday==True)
    """

    if df.empty:
        return pd.DataFrame(columns=["weekday", "count"])

    na_df = df[(df["minutes"] == 0) & (df["is_weekday"])].copy()
    if na_df.empty:
        return pd.DataFrame(columns=["weekday", "count"])

    counts = (
        na_df.groupby("weekday")
        .size()
        .reindex(WEEKDAY_LABELS[:5], fill_value=0)
        .reset_index(name="count")
    )
    counts["weekday"] = pd.Categorical(counts["weekday"], categories=WEEKDAY_LABELS[:5], ordered=True)
    return counts[["weekday", "count"]].sort_values("weekday")


__all__ = [
    "ShiftParseConfig",
    "ShiftRecord",
    "WEEKDAY_LABELS",
    "WORKING_SLOTS_ORDER",
    "build_shift_record",
    "build_shift_records_from_rows",
    "to_dataframe",
    "weekly_employee_stats",
    "weekly_team_stats",
    "weekday_slot_stats",
    "weekday_slot_stats_working",
    "weekday_na_counts",
]
