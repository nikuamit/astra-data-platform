"""dq/report.py — HTML DQ report from DQResult list. Attach to Airflow logs or Slack."""
from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
from dq.models import DQResult

def render_html_report(
    results: list[DQResult],
    entity: str = "unknown",
    batch_date: str = "",
    output_path: str | Path | None = None,
) -> str:
    ts = batch_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = ""
    for r in results:
        status = "✅ PASS" if r.passed else "❌ FAIL"
        color  = "#d1fae5" if r.passed else "#fee2e2"
        rows += (
            f"<tr style='background:{color}'>"
            f"<td>{r.rule_name}</td><td>{status}</td>"
            f"<td>{r.total_rows:,}</td><td>{r.failed_rows:,}</td>"
            f"<td>{r.failure_rate:.2%}</td></tr>"
        )
    html = f"""<!DOCTYPE html><html><head><meta charset='UTF-8'>
<title>DQ Report — {entity}</title>
<style>body{{font-family:Inter,sans-serif;padding:32px;background:#f9fafb}}
h1{{font-size:1.4rem;color:#111}}
table{{border-collapse:collapse;width:100%;margin-top:24px}}
th{{background:#1d4ed8;color:#fff;padding:10px 14px;text-align:left;font-size:.82rem}}
td{{padding:9px 14px;font-size:.82rem;border-bottom:1px solid #e5e7eb}}</style>
</head><body>
<h1>Data Quality Report &mdash; <code>{entity}</code></h1>
<p style='color:#6b7280;font-size:.85rem'>Batch date: {ts} &nbsp;|&nbsp; {len(results)} rules &nbsp;|&nbsp;
{sum(1 for r in results if r.passed)} passed &nbsp;|&nbsp; {sum(1 for r in results if not r.passed)} failed</p>
<table><tr><th>Rule</th><th>Status</th><th>Total Rows</th><th>Failed Rows</th><th>Failure Rate</th></tr>
{rows}</table></body></html>"""
    if output_path:
        Path(output_path).write_text(html, encoding="utf-8")
    return html
