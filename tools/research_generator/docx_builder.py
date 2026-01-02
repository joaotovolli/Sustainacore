"""DOCX and chart generation."""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

LOGGER = logging.getLogger("research_generator.docx")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _render_chart(chart_data: Dict[str, Any], output_dir: str, report_key: str) -> str:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:
        raise RuntimeError("matplotlib_missing") from exc

    _ensure_dir(output_dir)
    chart_path = os.path.join(output_dir, f"{report_key}_chart.png")
    chart_type = chart_data.get("type", "line")
    x_vals = chart_data.get("x") or []
    y_vals = chart_data.get("y") or []

    plt.figure(figsize=(6.4, 3.6))
    if chart_type == "bar":
        plt.bar(x_vals, y_vals)
    else:
        plt.plot(x_vals, y_vals, marker="o")
    plt.title(chart_data.get("title") or "")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(chart_path, dpi=150)
    plt.close()
    return chart_path


def _add_table(document, table_rows: List[Dict[str, Any]]) -> None:
    if not table_rows:
        return
    columns = list(table_rows[0].keys())
    table = document.add_table(rows=1, cols=len(columns))
    hdr_cells = table.rows[0].cells
    for idx, col in enumerate(columns):
        hdr_cells[idx].text = str(col)
    for row in table_rows:
        cells = table.add_row().cells
        for idx, col in enumerate(columns):
            cells[idx].text = str(row.get(col, ""))


def build_docx(
    draft: Dict[str, Any],
    bundle: Dict[str, Any],
    report_key: str,
    output_dir: str,
) -> Dict[str, Any]:
    try:
        from docx import Document
    except Exception as exc:
        raise RuntimeError("python_docx_missing") from exc

    chart_path = _render_chart(bundle.get("chart_data", {}), output_dir, report_key)

    document = Document()
    document.add_heading(draft.get("headline") or "Research Update", level=0)

    for paragraph in draft.get("paragraphs", []):
        document.add_paragraph(paragraph)

    document.add_paragraph(draft.get("chart_caption") or "")
    if os.path.exists(chart_path):
        document.add_picture(chart_path)

    document.add_paragraph(draft.get("table_caption") or "")
    _add_table(document, bundle.get("table_rows", []))

    document.add_paragraph(
        "Disclaimer: Research and education only; not investment advice."
    )
    document.add_paragraph(f"Methodology: {bundle.get('methodology_url')}")

    output_name = f"{report_key}.docx"
    output_path = os.path.join(output_dir, output_name)
    document.save(output_path)

    with open(output_path, "rb") as handle:
        docx_bytes = handle.read()

    return {
        "docx_bytes": docx_bytes,
        "docx_name": output_name,
        "chart_path": chart_path,
    }
