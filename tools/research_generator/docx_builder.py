"""DOCX and chart generation."""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional


LOGGER = logging.getLogger("research_generator.docx")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _set_cell_shading(cell, color: str) -> None:
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn

    tc_pr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:fill"), color)
    tc_pr.append(shd)


def _set_cell_border(cell, color: str = "D0D0D0") -> None:
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn

    tc_pr = cell._tc.get_or_add_tcPr()
    borders = OxmlElement("w:tcBorders")
    for edge in ("top", "left", "bottom", "right"):
        element = OxmlElement(f"w:{edge}")
        element.set(qn("w:val"), "single")
        element.set(qn("w:sz"), "4")
        element.set(qn("w:space"), "0")
        element.set(qn("w:color"), color)
        borders.append(element)
    tc_pr.append(borders)


def _format_value(value: Any, fmt: Optional[str]) -> str:
    if value is None:
        return ""
    if fmt == "pct":
        return f"{float(value):.1f}%"
    if fmt == "delta_pct":
        num = float(value)
        sign = "+" if num >= 0 else ""
        return f"{sign}{num:.1f}%"
    if fmt == "delta_pp":
        num = float(value)
        sign = "+" if num >= 0 else ""
        return f"{sign}{num:.1f}pp"
    if fmt == "score":
        return f"{float(value):.2f}"
    if fmt == "score_signed":
        num = float(value)
        sign = "+" if num >= 0 else ""
        return f"{sign}{num:.2f}"
    if fmt == "ratio":
        return f"{float(value):.3f}"
    return str(value)


def _highlight_color(value: Any, rule: Dict[str, Any]) -> Optional[str]:
    if value is None:
        return None
    target = rule.get("column_value")
    if target is not None and value == target:
        return rule.get("color")
    if isinstance(value, (int, float)):
        lte = rule.get("lte")
        if lte is not None and float(value) <= lte:
            return rule.get("color")
        abs_gte = rule.get("abs_gte")
        if abs_gte is not None and abs(float(value)) >= abs_gte:
            return rule.get("color")
    return None


def _render_table(
    document,
    *,
    title: str,
    rows: List[Dict[str, Any]],
    formats: Optional[Dict[str, str]] = None,
    column_widths: Optional[Dict[str, float]] = None,
    callouts: Optional[List[str]] = None,
    highlight_rules: Optional[List[Dict[str, Any]]] = None,
    table_index: int = 1,
) -> bool:
    if not rows:
        return False

    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.shared import Inches, Pt

    columns = list(rows[0].keys())
    document.add_heading(title, level=2)
    table = document.add_table(rows=1, cols=len(columns))
    table.autofit = False

    header_cells = table.rows[0].cells
    for idx, col in enumerate(columns):
        header_cells[idx].text = str(col)
        header_cells[idx].paragraphs[0].runs[0].font.bold = True
        header_cells[idx].paragraphs[0].runs[0].font.size = Pt(9)
        header_cells[idx].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        _set_cell_shading(header_cells[idx], "E8EEF7")
        _set_cell_border(header_cells[idx])
        if column_widths and col in column_widths:
            header_cells[idx].width = Inches(column_widths[col])

    for row_idx, row in enumerate(rows, start=1):
        cells = table.add_row().cells
        for col_idx, col in enumerate(columns):
            value = row.get(col)
            fmt = formats.get(col) if formats else None
            text = _format_value(value, fmt)
            cells[col_idx].text = text
            cells[col_idx].paragraphs[0].runs[0].font.size = Pt(9)
            _set_cell_border(cells[col_idx])
            if column_widths and col in column_widths:
                cells[col_idx].width = Inches(column_widths[col])
            if row_idx % 2 == 0:
                _set_cell_shading(cells[col_idx], "F7F7F7")

            if highlight_rules:
                for rule in highlight_rules:
                    if rule.get("column") != col:
                        continue
                    color = _highlight_color(value, rule)
                    if color:
                        _set_cell_shading(cells[col_idx], color)

    if callouts:
        document.add_paragraph(f"Table {table_index} — Key takeaways")
        for note in callouts[:6]:
            para = document.add_paragraph(note, style="List Bullet")
            para.paragraph_format.space_after = Pt(2)

    return True


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
    series = chart_data.get("series") or []

    plt.figure(figsize=(6.8, 3.6))
    if chart_type == "bar":
        if series:
            width = 0.8 / max(len(series), 1)
            indices = list(range(len(x_vals)))
            for idx, entry in enumerate(series):
                values = entry.get("values") or []
                offsets = [i + idx * width for i in indices]
                plt.bar(offsets, values, width=width, label=entry.get("name") or f"Series {idx+1}")
            plt.xticks(
                [i + width * (len(series) - 1) / 2 for i in indices],
                x_vals,
                rotation=45,
                ha="right",
            )
            if len(series) > 1:
                plt.legend(fontsize="small")
        else:
            plt.bar(x_vals, chart_data.get("y") or [])
            plt.xticks(rotation=45, ha="right")
    elif chart_type == "box":
        data = [entry.get("values") or [] for entry in series]
        labels = [entry.get("name") or "" for entry in series]
        plt.boxplot(data, labels=labels)
    elif chart_type == "hist":
        data = [entry.get("values") or [] for entry in series]
        labels = [entry.get("name") or "" for entry in series]
        plt.hist(data, label=labels, bins=12, alpha=0.7)
        if len(labels) > 1:
            plt.legend(fontsize="small")
    else:
        for idx, entry in enumerate(series or []):
            values = entry.get("values") or []
            plt.plot(x_vals, values, marker="o", label=entry.get("name") or f"Series {idx+1}")
        if series and len(series) > 1:
            plt.legend(fontsize="small")

    plt.title(chart_data.get("title") or "")
    if chart_data.get("y_label"):
        plt.ylabel(chart_data.get("y_label"))
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(chart_path, dpi=150)
    plt.close()
    return chart_path


def build_docx(
    draft: Dict[str, Any],
    bundle: Dict[str, Any],
    report_key: str,
    output_dir: str,
) -> Dict[str, Any]:
    try:
        from docx import Document
        from docx.shared import Inches
    except Exception as exc:
        raise RuntimeError("python_docx_missing") from exc

    chart_paths: List[str] = []
    chart_blocks = bundle.get("docx_charts") or []
    if chart_blocks:
        for idx, chart in enumerate(chart_blocks):
            chart_paths.append(_render_chart(chart, output_dir, f"{report_key}_{idx}"))
    else:
        chart_paths.append(_render_chart(bundle.get("chart_data", {}), output_dir, report_key))

    document = Document()
    document.add_heading(draft.get("headline") or "Research Update", level=0)

    for paragraph in draft.get("paragraphs", []):
        text = str(paragraph)
        if text.strip().startswith("- "):
            document.add_paragraph(text.strip()[2:], style="List Bullet")
        else:
            document.add_paragraph(text)

    table_style_applied = False

    if chart_blocks:
        for idx, chart in enumerate(chart_blocks, start=1):
            document.add_paragraph(chart.get("caption") or draft.get("chart_caption") or "")
            path = chart_paths[idx - 1]
            if os.path.exists(path):
                document.add_picture(path, width=Inches(6.5))
            callouts = chart.get("callouts") or []
            if callouts:
                document.add_paragraph(f"Figure {idx} — What it shows")
                for note in callouts[:6]:
                    document.add_paragraph(note, style="List Bullet")
    else:
        document.add_paragraph(draft.get("chart_caption") or "")
        if os.path.exists(chart_paths[0]):
            document.add_picture(chart_paths[0], width=Inches(6.5))

    tables = bundle.get("docx_tables") or []
    if tables:
        for idx, table in enumerate(tables, start=1):
            table_style_applied = _render_table(
                document,
                title=table.get("title") or f"Table {idx}",
                rows=table.get("rows", []),
                formats=table.get("formats"),
                column_widths=table.get("column_widths"),
                callouts=table.get("callouts"),
                highlight_rules=table.get("highlight_rules"),
                table_index=idx,
            ) or table_style_applied
    else:
        document.add_paragraph(draft.get("table_caption") or "")
        if bundle.get("table_rows"):
            table_style_applied = _render_table(
                document,
                title="Table",
                rows=bundle.get("table_rows", []),
                table_index=1,
            )

    document.add_paragraph("Disclaimer: Research and education only; not investment advice.")
    document.add_paragraph(f"Methodology: {bundle.get('methodology_url')}")

    output_name = f"{report_key}.docx"
    output_path = os.path.join(output_dir, output_name)
    document.save(output_path)

    with open(output_path, "rb") as handle:
        docx_bytes = handle.read()

    return {
        "docx_bytes": docx_bytes,
        "docx_name": output_name,
        "chart_path": chart_paths[0] if chart_paths else "",
        "table_style_applied": table_style_applied,
        "table_count": len(tables) if tables else (1 if bundle.get("table_rows") else 0),
        "figure_count": len(chart_blocks) if chart_blocks else (1 if chart_paths else 0),
    }
