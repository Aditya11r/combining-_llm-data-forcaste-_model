from __future__ import annotations

import html
from io import BytesIO
from textwrap import wrap

from app.schemas import AnalysisResponse


def build_report_html(result: AnalysisResponse) -> str:
    report = result.consultant_report
    cluster = result.cluster
    extracted = result.extracted_kpis
    quality = result.extraction_quality
    years = [
        str(record.fiscal_year_start or record.fiscal_year)
        for record in extracted.yearly_records
        if record.fiscal_year_start or record.fiscal_year
    ]
    years_label = ", ".join(years) or extracted.fiscal_year or "Unknown"

    kpi_rows = _html_rows(
        [
            ("Scope 1", _fmt(extracted.scope1_tco2e), "tCO2e"),
            ("Scope 2", _fmt(extracted.scope2_tco2e), "tCO2e"),
            ("Total Scope 1 + 2", _fmt(extracted.total_scope1_scope2_tco2e), "tCO2e"),
            ("Water consumption", _fmt(extracted.water_consumption_kl), "kL"),
            ("Waste generated", _fmt(extracted.waste_generated_tonnes), "tonnes"),
            ("Waste recycled", _fmt(extracted.waste_recycled_tonnes), "tonnes"),
        ]
    )
    forecast_rows = _html_rows(
        [(str(point.year), _fmt(point.total_scope1_scope2_tco2e), point.source) for point in result.forecast]
    )
    yearly_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(record.fiscal_year_start or record.fiscal_year or 'Unknown'))}</td>"
        f"<td>{html.escape(_fmt(record.scope1_tco2e))}</td>"
        f"<td>{html.escape(_fmt(record.scope2_tco2e))}</td>"
        f"<td>{html.escape(_fmt(record.computed_total_scope1_scope2_tco2e))}</td>"
        f"<td>{html.escape(_fmt(record.water_consumption_kl))}</td>"
        f"<td>{html.escape(_fmt(record.waste_generated_tonnes))}</td>"
        f"<td>{html.escape(_fmt(record.waste_recycled_tonnes))}</td>"
        "</tr>"
        for record in extracted.yearly_records
    )
    if not yearly_rows:
        yearly_rows = "<tr><td colspan=\"7\">No yearly KPI records available</td></tr>"
    peer_rows = _html_rows(
        [(label, _fmt(value), "") for label, value in result.peer_comparison.averages.items()]
    )

    risks = "".join(f"<li>{html.escape(item)}</li>" for item in report.risks)
    recommendations = "".join(f"<li>{html.escape(item)}</li>" for item in report.recommendations)
    chart_notes = "".join(f"<li>{html.escape(item)}</li>" for item in report.chart_narratives)
    notes = "".join(f"<li>{html.escape(item)}</li>" for item in quality.notes)
    missing = ", ".join(quality.missing_required_fields) or "None"
    sample_companies = ", ".join(result.peer_comparison.sample_companies[:12]) or "No sample companies available"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>ESG Consultant Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 0; color: #172026; line-height: 1.5; background: #f4f7f8; }}
    main {{ max-width: 1040px; margin: 0 auto; padding: 40px; background: #fff; min-height: 100vh; }}
    h1 {{ margin: 0; color: #102a43; font-size: 30px; }}
    h2 {{ color: #102a43; margin-top: 30px; border-top: 1px solid #d9e2e5; padding-top: 20px; }}
    .meta {{ color: #52616b; margin-bottom: 24px; }}
    .metric-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin: 24px 0; }}
    .metric {{ border: 1px solid #d9e2e5; border-radius: 8px; padding: 14px; background: #fbfcfd; }}
    .label {{ font-size: 12px; color: #52616b; text-transform: uppercase; }}
    .value {{ font-size: 22px; font-weight: 700; }}
    table {{ width: 100%; border-collapse: collapse; margin: 12px 0; }}
    th, td {{ border-bottom: 1px solid #e7eaee; text-align: left; padding: 10px 8px; font-size: 14px; }}
    th {{ color: #52616b; font-size: 12px; text-transform: uppercase; }}
    .note {{ background: #fff7ed; border: 1px solid #fed7aa; border-radius: 8px; padding: 12px; }}
    @media (max-width: 800px) {{ main {{ padding: 24px; }} .metric-grid {{ grid-template-columns: 1fr 1fr; }} }}
  </style>
</head>
<body>
<main>
  <h1>ESG Consultant Report</h1>
  <p class="meta">Session {html.escape(result.session_id)} | {html.escape(extracted.company_name or "Unknown company")}</p>
  <div class="metric-grid">
    <div class="metric"><div class="label">Latest Year</div><div class="value">{html.escape(extracted.fiscal_year or "Unknown")}</div></div>
    <div class="metric"><div class="label">Years Covered</div><div class="value">{len(years) or 1}</div></div>
    <div class="metric"><div class="label">Cluster</div><div class="value">{cluster.KMeans_cluster}</div></div>
    <div class="metric"><div class="label">Quality</div><div class="value">{quality.level}</div></div>
  </div>
  <p class="meta"><strong>Years analyzed:</strong> {html.escape(years_label)}</p>
  <h2>Executive Summary</h2>
  <p>{html.escape(report.executive_summary)}</p>
  <h2>Latest KPI Snapshot</h2>
  <table><thead><tr><th>Metric</th><th>Value</th><th>Unit / Source</th></tr></thead><tbody>{kpi_rows}</tbody></table>
  <h2>Multi-Year KPI Trend Inputs</h2>
  <table><thead><tr><th>Year</th><th>Scope 1</th><th>Scope 2</th><th>Total Scope 1 + 2</th><th>Water</th><th>Waste Generated</th><th>Waste Recycled</th></tr></thead><tbody>{yearly_rows}</tbody></table>
  <h2>Cluster Interpretation</h2>
  <p>{html.escape(report.cluster_interpretation)}</p>
  <p><strong>{html.escape(cluster.KMeans_cluster_label)}</strong></p>
  <h2>Forecast</h2>
  <p>{html.escape(report.forecast_interpretation)}</p>
  <table><thead><tr><th>Year</th><th>Total Scope 1 + 2</th><th>Source</th></tr></thead><tbody>{forecast_rows}</tbody></table>
  <h2>Peer Benchmark</h2>
  <p>{html.escape(report.peer_benchmark)}</p>
  <p><strong>Sample peer companies:</strong> {html.escape(sample_companies)}</p>
  <table><thead><tr><th>Peer metric</th><th>Average</th><th></th></tr></thead><tbody>{peer_rows}</tbody></table>
  <h2>Chart Reading Notes</h2>
  <ul>{chart_notes}</ul>
  <h2>Risks</h2>
  <ul>{risks}</ul>
  <h2>Evidence-Based Recommendations</h2>
  <ul>{recommendations}</ul>
  <h2>Extraction Quality</h2>
  <p class="note"><strong>Missing required fields:</strong> {html.escape(missing)}</p>
  <ul>{notes}</ul>
  <h2>Confidence Note</h2>
  <p>{html.escape(report.confidence_note)}</p>
</main>
</body>
</html>"""


def build_simple_pdf(result: AnalysisResponse) -> bytes:
    extracted = result.extracted_kpis
    quality = result.extraction_quality
    report = result.consultant_report
    years = [
        str(record.fiscal_year_start or record.fiscal_year)
        for record in extracted.yearly_records
        if record.fiscal_year_start or record.fiscal_year
    ]

    lines = [
        "ESG Consultant Report",
        f"Session: {result.session_id}",
        f"Company: {extracted.company_name or 'Unknown company'}",
        f"Latest fiscal year: {extracted.fiscal_year or 'Unknown'}",
        f"Years analyzed: {', '.join(years) or extracted.fiscal_year or 'Unknown'}",
        f"Cluster: {result.cluster.KMeans_cluster} - {result.cluster.KMeans_cluster_label}",
        f"Extraction quality: {quality.level} ({quality.score})",
        "",
        "Executive Summary",
        report.executive_summary,
        "",
        "Latest KPI Snapshot",
        f"Scope 1: {_fmt(extracted.scope1_tco2e)} tCO2e",
        f"Scope 2: {_fmt(extracted.scope2_tco2e)} tCO2e",
        f"Total Scope 1 + 2: {_fmt(extracted.total_scope1_scope2_tco2e)} tCO2e",
        f"Water consumption: {_fmt(extracted.water_consumption_kl)} kL",
        f"Waste generated: {_fmt(extracted.waste_generated_tonnes)} tonnes",
        f"Waste recycled: {_fmt(extracted.waste_recycled_tonnes)} tonnes",
        "",
        "Multi-Year KPI Trend Inputs",
        *[
            f"{record.fiscal_year_start or record.fiscal_year}: "
            f"scope 1 {_fmt(record.scope1_tco2e)}, "
            f"scope 2 {_fmt(record.scope2_tco2e)}, "
            f"total emissions {_fmt(record.computed_total_scope1_scope2_tco2e)}, "
            f"water {_fmt(record.water_consumption_kl)}, "
            f"waste {_fmt(record.waste_generated_tonnes)}, "
            f"recycled {_fmt(record.waste_recycled_tonnes)}"
            for record in extracted.yearly_records
        ],
        "",
        "Cluster Interpretation",
        report.cluster_interpretation,
        "",
        "Forecast Interpretation",
        report.forecast_interpretation,
        *[f"{point.year}: {_fmt(point.total_scope1_scope2_tco2e)} tCO2e ({point.source})" for point in result.forecast],
        "",
        "Peer Benchmark",
        report.peer_benchmark,
        f"Sample peers: {', '.join(result.peer_comparison.sample_companies[:8]) or 'None available'}",
        *[f"{key}: {_fmt(value)}" for key, value in result.peer_comparison.averages.items()],
        "",
        "Chart Reading Notes",
        *[f"- {item}" for item in report.chart_narratives],
        "",
        "Risks",
        *[f"- {item}" for item in report.risks],
        "",
        "Evidence-Based Recommendations",
        *[f"- {item}" for item in report.recommendations],
        "",
        "Extraction Quality Notes",
        f"Missing required fields: {', '.join(quality.missing_required_fields) or 'None'}",
        *[f"- {item}" for item in quality.notes],
        "",
        "Confidence Note",
        report.confidence_note,
    ]
    wrapped = []
    for line in lines:
        wrapped.extend(wrap(str(line), width=92) or [""])

    return _pdf_from_lines(wrapped)


def _pdf_from_lines(lines: list[str]) -> bytes:
    pages = [lines[index : index + 52] for index in range(0, len(lines), 52)] or [[]]
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"",  # pages tree placeholder
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    page_object_ids: list[int] = []
    for page_lines in pages:
        stream_lines = ["BT", "/F1 10 Tf", "50 780 Td", "14 TL"]
        first = True
        for line in page_lines:
            escaped = _escape_pdf_text(line)
            if first:
                stream_lines.append(f"({escaped}) Tj")
                first = False
            else:
                stream_lines.append(f"T* ({escaped}) Tj")
        stream_lines.append("ET")
        content = "\n".join(stream_lines).encode("latin-1", errors="replace")
        content_id = len(objects) + 2
        page_id = len(objects) + 1
        page_object_ids.append(page_id)
        objects.append(
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_id} 0 R >>".encode("ascii")
        )
        objects.append(
            b"<< /Length " + str(len(content)).encode("ascii") + b" >>\nstream\n" + content + b"\nendstream"
        )

    kids = " ".join(f"{page_id} 0 R" for page_id in page_object_ids)
    objects[1] = f"<< /Type /Pages /Kids [{kids}] /Count {len(page_object_ids)} >>".encode("ascii")

    output = BytesIO()
    output.write(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(output.tell())
        output.write(f"{index} 0 obj\n".encode("ascii"))
        output.write(obj)
        output.write(b"\nendobj\n")

    xref_start = output.tell()
    output.write(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    output.write(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.write(f"{offset:010d} 00000 n \n".encode("ascii"))

    output.write(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF".encode("ascii")
    )
    return output.getvalue()


def _escape_pdf_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _fmt(value) -> str:
    if value is None:
        return "Not available"
    if isinstance(value, (int, float)):
        return f"{value:,.2f}"
    return str(value)


def _html_rows(rows: list[tuple[str, str, str]]) -> str:
    if not rows:
        return "<tr><td colspan=\"3\">No data available</td></tr>"
    return "".join(
        "<tr>"
        f"<td>{html.escape(str(first))}</td>"
        f"<td>{html.escape(str(second))}</td>"
        f"<td>{html.escape(str(third))}</td>"
        "</tr>"
        for first, second, third in rows
    )
