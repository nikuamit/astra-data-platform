"""
dq/report.py
Converts DQResult list → a clean HTML report file.
Useful for attaching to Airflow task logs, Slack notifications, or PR comments.

Usage:
    from dq.report import render_html_report
    render_html_report(results, entity="events", output_path="/tmp/dq_report.html")
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from dq.models import DQResult


_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>DQ Report — {entity}</title>
<style>
  body {{ font-family: -apple-system, sans-serif; margin: 2rem; color: #1a1a1a; }}
  h1   {{ font-size: 1.4rem; margin-bottom: 0.3rem; }}
  p.meta {{ color: #666; font-size: 0.85rem; margin-top: 0; }}
  table {{ border-collapse: collapse; width: 100%; margin-top: 1.5rem; font-size: 0.9rem; }}
  th   {{ background: #1e293b; color: #fff; padding: 0.6rem 1rem; text-align: left; }}
  td   {{ padding: 0.55rem 1rem; border-bottom: 1px solid #e5e7eb; }}
  tr:hover td {{ background: #f8fafc; }}
  .pass {{ color: #16a34a; font-weight: 600; }}
  .fail {{ color: #dc2626; font-weight: 600; }}
  .summary {{ display: flex; gap: 2rem; margin-top: 1.5rem; }}
  .card {{ border: 1px solid #e5e7eb; border-radius: 8px; padding: 1rem 1.5rem; min-width: 120px; }}
  .card .val {{ font-size: 2rem; font-weight: 700; }}
  .card .lbl {{ font-size: 0.8rem; color: #666; }}
</style>
</head>
<body>
<h1>Data Quality Report — <code>{entity}</code></h1>
<p class="meta">Generated: {timestamp} &nbsp;|&nbsp; Batch date: {batch_date}</p>

<div class="summary">
  <div class="card"><div class="val">{total_rules}</div><div class="lbl">Rules run</div></div>
  <div class="card"><div class="val" style="color:#16a34a">{passed}</div><div class="lbl">Passed</div></div>
  <div class="card"><div class="val" style="color:#dc2626">{failed}</div><div class="lbl">Failed</div></div>
  <div class="card"><div class="val">{total_rows:,}</div><div class="lbl">Total rows</div></div>
</div>

<table>
  <thead>
    <tr>
      <th>Rule</th><th>Status</th><th>Total rows</th>
      <th>Failed rows</th><th>Failure rate</th>
    </tr>
  </thead>
  <tbody>
{rows}
  </tbody>
</table>
</body>
</html>
"""

_ROW_TEMPLATE = """\
    <tr>
      <td>{rule_name}</td>
      <td class="{css}">{status}</td>
      <td>{total_rows:,}</td>
      <td>{failed_rows:,}</td>
      <td>{failure_rate:.2%}</td>
    </tr>"""


def render_html_report(
    results: list[DQResult],
    entity: str,
    batch_date: str = "",
    output_path: str | Path | None = None,
) -> str:
    """
    Render DQ results as an HTML string and optionally write to a file.
    Returns the HTML string regardless.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    total_rows = results[0].total_rows if results else 0

    row_html = "\n".join(
        _ROW_TEMPLATE.format(
            rule_name=r.rule_name,
            css="pass" if r.passed else "fail",
            status="✅ PASS" if r.passed else "❌ FAIL",
            total_rows=r.total_rows,
            failed_rows=r.failed_rows,
            failure_rate=r.failure_rate,
        )
        for r in results
    )

    html = _HTML_TEMPLATE.format(
        entity=entity,
        timestamp=now,
        batch_date=batch_date or "—",
        total_rules=len(results),
        passed=passed,
        failed=failed,
        total_rows=total_rows,
        rows=row_html,
    )

    if output_path:
        Path(output_path).write_text(html, encoding="utf-8")

    return html
