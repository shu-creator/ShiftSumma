from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List

import matplotlib as mpl
from matplotlib import font_manager


# フォントを最初に設定してから pyplot を読み込む
FONT_PATH = Path(__file__).resolve().parent / "assets" / "NotoSansJP-VariableFont_wght.ttf"

if FONT_PATH.exists():
    font_manager.fontManager.addfont(str(FONT_PATH))
    mpl.rcParams["font.family"] = "Noto Sans JP"
    mpl.rcParams["axes.unicode_minus"] = False
else:
    print("WARN: Noto Sans JP font not found:", FONT_PATH)

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from analytics.stats import (
    ShiftParseConfig,
    WEEKDAY_LABELS,
    build_shift_records_from_rows,
    to_dataframe,
    weekly_employee_stats,
    weekday_slot_stats,
)
from parsers.excel_parser import ExcelShiftParser
from parsers.pdf_parser import PdfShiftParser


PAGE_TITLE = "シフト管理・分析ダッシュボード"
SAMPLE_EMPLOYEES = ["101", "102", "201"]


@st.cache_data
def generate_sample_records(target_month: str) -> pd.DataFrame:
    """デモ用のシフトデータを生成。"""

    month_start = pd.to_datetime(f"{target_month}-01")
    month_end = (month_start + pd.offsets.MonthEnd(0)).date()
    dates = pd.date_range(month_start, month_end, freq="D").date

    rows = []
    for emp in SAMPLE_EMPLOYEES:
        for d in dates:
            if d.weekday() >= 5:
                continue
            start = "09:00" if d.weekday() % 2 == 0 else "13:30"
            end = "18:00" if start == "09:00" else "17:00"
            rows.append(
                {
                    "employee_id": emp,
                    "date": d,
                    "start_time": start,
                    "end_time": end,
                    "raw_status": None,
                }
            )
    config = ShiftParseConfig()
    records = build_shift_records_from_rows(rows, config)
    return to_dataframe(records)


def parse_uploaded_file(upload, target_month: str, config: ShiftParseConfig) -> pd.DataFrame:
    suffix = Path(upload.name).suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        parser = ExcelShiftParser(config)
        return parser.read(upload)
    if suffix == ".pdf":
        parser = PdfShiftParser(config)
        return parser.read(upload, target_month)
    st.warning("PDF か Excel ファイルをアップロードしてください。")
    return pd.DataFrame()


def apply_exclusions(df: pd.DataFrame, exclude_ids: List[str]) -> pd.DataFrame:
    if not exclude_ids or df.empty:
        return df
    return df[~df["employee_id"].astype(str).isin(exclude_ids)].copy()


def compute_warning(df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    missing_start = df["start_time"].isna().sum()
    missing_end = df["end_time"].isna().sum()
    zero_minutes = (df["minutes"] <= 0).sum()
    return f"入時刻欠損: {missing_start}件 / 退時刻欠損: {missing_end}件 / 実働0分: {zero_minutes}件"


def plot_employee_trend(stats_df: pd.DataFrame, employee: str, target_hours: float):
    emp_df = stats_df[stats_df["employee_id"] == employee]
    fig, ax = plt.subplots()
    ax.plot(emp_df["week_index"], emp_df["week_hours"], marker="o", label="週実働時間")
    ax.axhline(target_hours, color="red", linestyle="--", label="目標")
    ax.set_xlabel("週")
    ax.set_ylabel("時間")
    ax.set_title(f"社員 {employee} の週別実働時間")
    ax.legend()
    ax.grid(True, linestyle=":", alpha=0.5)
    return fig


def plot_employee_heatmap(stats_df: pd.DataFrame):
    if stats_df.empty:
        return None
    pivot = stats_df.pivot(index="employee_id", columns="week_index", values="week_hours").fillna(0)
    fig, ax = plt.subplots()
    cax = ax.imshow(pivot.values, aspect="auto")
    ax.set_xticks(range(pivot.shape[1]))
    ax.set_xticklabels(pivot.columns)
    ax.set_yticks(range(pivot.shape[0]))
    ax.set_yticklabels(pivot.index)
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            ax.text(j, i, f"{pivot.values[i, j]:.1f}", ha="center", va="center", color="white")
    fig.colorbar(cax, ax=ax, label="週実働時間")
    ax.set_xlabel("週")
    ax.set_ylabel("社員")
    ax.set_title("社員×週 実働時間ヒートマップ")
    return fig


def plot_weekday_slot_heatmap(slot_df: pd.DataFrame):
    if slot_df.empty:
        return None
    ordered = slot_df.pivot(index="weekday", columns="slot", values="count").reindex(WEEKDAY_LABELS[:5])
    fig, ax = plt.subplots()
    data = ordered.fillna(0).values
    cax = ax.imshow(data, aspect="auto")
    ax.set_xticks(range(ordered.shape[1]))
    ax.set_xticklabels(ordered.columns)
    ax.set_yticks(range(ordered.shape[0]))
    ax.set_yticklabels(ordered.index)
    for i in range(ordered.shape[0]):
        for j in range(ordered.shape[1]):
            value = data[i, j]
            color = "white" if value > data.max() * 0.6 else "black"
            ax.text(j, i, int(value), ha="center", va="center", color=color)
    fig.colorbar(cax, ax=ax, label="件数")
    ax.set_xlabel("時間帯")
    ax.set_ylabel("曜日")
    ax.set_title("曜日×時間帯 ヒートマップ")
    return fig


def export_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)

    st.sidebar.header("入力設定")
    uploaded = st.sidebar.file_uploader("シフトファイルをアップロード", type=["pdf", "xlsx", "xls"])
    target_month = st.sidebar.text_input("対象年月 (YYYY-MM)", value=datetime.today().strftime("%Y-%m"))
    full_threshold = st.sidebar.number_input("Full判定閾値(分)", value=270, step=30)
    half_threshold = st.sidebar.number_input("半日判定閾値(分)", value=180, step=30)
    exclude_input = st.sidebar.text_input("除外社員ID(カンマ区切り)")
    exclude_ids = [x.strip() for x in exclude_input.split(",") if x.strip()]

    run_button = st.sidebar.button("集計実行")
    sample_button = st.sidebar.button("サンプルデータで試す")

    config = ShiftParseConfig(full_threshold_minutes=int(full_threshold), half_min_minutes=int(half_threshold))

    if "shift_df" not in st.session_state:
        st.session_state.shift_df = pd.DataFrame()

    if run_button and uploaded:
        shift_df = parse_uploaded_file(uploaded, target_month, config)
        shift_df = apply_exclusions(shift_df, exclude_ids)
        st.session_state.shift_df = shift_df
    elif sample_button:
        st.session_state.shift_df = generate_sample_records(target_month)

    shift_df = st.session_state.shift_df

    st.subheader("A. データ読み込み・フィルタ")
    if shift_df.empty:
        st.info("左側のアップロードまたはサンプルボタンでデータを読み込んでください。")
        return

    st.write(f"ShiftRecord 件数: {len(shift_df)}")
    st.warning(compute_warning(shift_df))

    tabs = st.tabs(
        [
            "社員×週の実働時間",
            "チーム曜日×時間帯",
            "データエクスポート",
        ]
    )

    with tabs[0]:
        st.subheader("B. 社員別×週別の実働時間・フェアネス")
        weekly_emp = weekly_employee_stats(shift_df)
        st.dataframe(weekly_emp)

        target_hours = st.number_input("社員共通 目標週時間", value=20.0, step=1.0)
        employees = weekly_emp["employee_id"].unique().tolist()
        if employees:
            selected_emp = st.selectbox("表示する社員", employees)
            fig = plot_employee_trend(weekly_emp, selected_emp, target_hours)
            st.pyplot(fig)

            heatmap_fig = plot_employee_heatmap(weekly_emp)
            if heatmap_fig:
                st.pyplot(heatmap_fig)
        else:
            st.info("社員データがありません")

    with tabs[1]:
        st.subheader("C. 曜日×時間帯のシフト配置")
        slot_df = weekday_slot_stats(shift_df)
        st.dataframe(slot_df)
        heatmap = plot_weekday_slot_heatmap(slot_df)
        if heatmap:
            st.pyplot(heatmap)
        st.caption("将来的に曜日別の目標人数を追加し、差分を可視化できる設計にしています。")

    with tabs[2]:
        st.subheader("D. データエクスポート")
        weekly_emp = weekly_employee_stats(shift_df)
        slot_df = weekday_slot_stats(shift_df)
        st.download_button("ShiftRecord CSV", data=export_csv(shift_df), file_name="shift_records.csv")
        st.download_button("WeeklyEmployeeStats CSV", data=export_csv(weekly_emp), file_name="weekly_employee_stats.csv")
        st.download_button("WeekdaySlotStats CSV", data=export_csv(slot_df), file_name="weekday_slot_stats.csv")


if __name__ == "__main__":
    main()
