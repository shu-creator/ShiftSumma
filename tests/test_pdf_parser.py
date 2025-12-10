from __future__ import annotations

import unittest
from pathlib import Path

try:
    import pandas as pd
except ImportError:  # 環境に依存するため、無ければテストをスキップ
    pd = None

EXPECTED_ROWS = [
    ("234198", "2025-12-01", 240, "PM半日"),
    ("234198", "2025-12-03", 450, "Full"),
    ("234198", "2025-12-04", 0, "NA"),
    ("243458", "2025-12-01", 240, "AM半日"),
    ("243458", "2025-12-03", 510, "Full"),
    ("253712", "2025-12-02", 240, "PM半日"),
    ("253712", "2025-12-04", 480, "Full"),
]


class PdfParserIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.pdf_path = Path(__file__).resolve().parent.parent / "2025-12-shift.pdf.pdf"
        if pd is None:
            self.skipTest("pandas not installed; skipping PDF integration test.")
        try:
            from parsers.pdf_parser import PdfShiftParser  # noqa: WPS433
            import pdfplumber  # noqa: WPS433
        except Exception:
            self.skipTest("pdfplumber not available; skipping PDF integration test.")
        if not self.pdf_path.exists():
            self.skipTest("Sample PDF not available in repository; skipping PDF integration test.")

        self.parser_cls = PdfShiftParser

    def test_expected_minutes_and_slots(self) -> None:
        parser = self.parser_cls()
        df = parser.read(str(self.pdf_path), "2025-12")

        self.assertFalse((df["employee_id"] == "0").any())

        for employee_id, date_str, minutes, slot in EXPECTED_ROWS:
            with self.subTest(employee_id=employee_id, date=date_str):
                target_date = pd.to_datetime(date_str).date()
                row = df[(df["employee_id"] == employee_id) & (df["date"] == target_date)]
                self.assertFalse(row.empty, msg=f"missing row for {employee_id} {date_str}")
                self.assertEqual(int(row.iloc[0]["minutes"]), minutes)
                self.assertEqual(row.iloc[0]["slot"], slot)


if __name__ == "__main__":
    unittest.main()
