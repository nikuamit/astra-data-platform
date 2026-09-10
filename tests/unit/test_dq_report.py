"""
tests/unit/test_dq_report.py
"""

from dq.models import DQResult
from dq.report import render_html_report


def _make_results():
    return [
        DQResult("event_id_not_null", passed=True,  total_rows=1000, failed_rows=0,  failure_rate=0.0),
        DQResult("amount_range",      passed=False, total_rows=1000, failed_rows=42, failure_rate=0.042),
    ]


def test_render_returns_html():
    html = render_html_report(_make_results(), entity="events", batch_date="2024-01-01")
    assert "<table>" in html
    assert "events" in html
    assert "✅ PASS" in html
    assert "❌ FAIL" in html


def test_render_shows_correct_counts():
    html = render_html_report(_make_results(), entity="events")
    assert "42" in html        # failed rows
    assert "1,000" in html     # total rows formatted


def test_render_writes_file(tmp_path):
    out = tmp_path / "report.html"
    render_html_report(_make_results(), entity="events", output_path=out)
    assert out.exists()
    assert out.stat().st_size > 0
