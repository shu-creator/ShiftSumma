# ShiftSumma

Streamlit ベースのシフト管理・分析アプリのたたき台です。PDF/Excel から ShiftRecord を構築し、週次・曜日別の集計や可視化、CSV エクスポートを提供します。

## ディレクトリ構成案
- `app.py`: Streamlit UI と画面遷移、可視化のエントリーポイント。
- `analytics/`
  - `models.py`: ShiftRecord / 集計結果のデータクラス定義。
  - `stats.py`: 実働分計算、週番号算出、集計ロジック。
- `parsers/`
  - `excel_parser.py`: Excel からシフト表を読み込む小さな変換レイヤー。
  - `pdf_parser.py`: pdfplumber を使った座標ベースの暫定パーサー。
- `assets/`: Noto Sans JP などフォントを配置する想定のディレクトリ。
- `requirements.txt`: 依存ライブラリ一覧。

## 主なクラス・関数
- `ShiftRecord` (analytics.models): 社員×日単位のコアデータモデル。
- `ShiftParseConfig` (analytics.models): Full/半日判定の閾値設定。
- `build_shift_record` / `build_shift_records_from_rows` (analytics.stats): 行データから ShiftRecord を構築。
- `weekly_employee_stats` / `weekly_team_stats` / `weekday_slot_stats` (analytics.stats): 週別・曜日別の集計。
- `ExcelShiftParser.read` / `PdfShiftParser.read`: アップロードファイルから ShiftRecord DataFrame を生成。
- `app.py` 内の `plot_*` 系: 週次折れ線、社員×週ヒートマップ、曜日×時間帯ヒートマップ描画。

## 使い方 (ローカル実行)
1. 依存をインストール
   ```bash
   pip install -r requirements.txt
   ```
2. Streamlit 起動
   ```bash
   streamlit run app.py
   ```
3. 画面左で PDF/Excel をアップロードまたはサンプルデータを生成し、集計を実行してください。

フォント `assets/NotoSansJP-Regular.ttf` を配置すると matplotlib のラベルが日本語で崩れにくくなります。
