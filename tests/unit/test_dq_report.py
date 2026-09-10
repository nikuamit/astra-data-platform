"""tests/unit/test_dq_report.py"""
from dq.models import DQResult
from dq.report import render_html_report

def _results():
    return [
        DQResult("event_id_not_null", passed=True,  total_rows=1000, failed_rows=0,  failure_rate=0.0),
        DQResult("amount_range",      passed=False, total_rows=1000, failed_rows=42, failure_rate=0.042),
    ]

def test_render_returns_html():
    html = render_html_report(_results(), entity="events", batch_date="2024-01-01")
    assert "<table>" in html and "events" in html
    assert "✅ PASS" in html and "❌ FAIL" in html

def test_render_shows_counts():
    html = render_html_report(_results(), entity="events")
    assert "42" in html and "1,000" in html

def test_render_writes_file(tmp_path):
    out = tmp_path / "report.html"
    render_html_report(_results(), entity="events", output_path=out)
    assert out.exists() and out.stat().st_size > 0
