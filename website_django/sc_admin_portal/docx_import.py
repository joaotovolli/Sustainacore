from __future__ import annotations

import re
from html import escape
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from docx import Document
from docx.table import Table
from docx.text.paragraph import Paragraph
from docx.text.run import Run
_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_HYPERLINK_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


def _iter_block_items(document: Document) -> Iterable[Paragraph | Table]:
    for child in document.element.body.iterchildren():
        if child.tag.endswith("}p"):
            yield Paragraph(child, document)
        elif child.tag.endswith("}tbl"):
            yield Table(child, document)


def _heading_level(paragraph: Paragraph) -> Optional[int]:
    style_name = (paragraph.style.name if paragraph.style else "") or ""
    normalized = style_name.strip().lower()
    if normalized == "title":
        return 1
    match = re.search(r"heading\\s*(\\d+)", normalized)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _format_run_text(run) -> str:
    raw_text = run.text or ""
    if not raw_text:
        return ""
    escaped = escape(raw_text).replace("\n", "<br>")
    if run.bold and run.italic:
        return f"<strong><em>{escaped}</em></strong>"
    if run.bold:
        return f"<strong>{escaped}</strong>"
    if run.italic:
        return f"<em>{escaped}</em>"
    return escaped


def _resolve_hyperlink_target(document: Document, rel_id: str) -> Optional[str]:
    if not rel_id:
        return None
    rel = document.part.rels.get(rel_id)
    if not rel:
        return None
    target = rel.target_ref or ""
    target = target.strip()
    if not target:
        return None
    if target.startswith("http://") or target.startswith("https://") or target.startswith("/"):
        return target
    if target.startswith("mailto:"):
        return target
    return f"/{target.lstrip('/')}"


def _auto_link_internal_company_label(raw_text: str, html: str) -> str:
    if "<a " in html:
        return html
    match = re.search(r"/tech100/company/([A-Z0-9]+)/", raw_text or "")
    if not match:
        return html
    ticker = match.group(1)
    label_match = re.search(rf"([A-Za-z0-9 .,&()\\-]+\\({ticker}\\):?)", raw_text)
    if not label_match:
        return html
    label = label_match.group(1)
    escaped_label = escape(label)
    if escaped_label not in html:
        return html
    return html.replace(
        escaped_label,
        f'<a href="/tech100/company/{ticker}/">{escaped_label}</a>',
        1,
    )


def _extract_run_images(
    *,
    run,
    document: Document,
    image_cache: Dict[str, Tuple[int, str]],
    asset_uploader: Callable[[str | None, str | None, bytes], int],
    image_stats: Optional[Dict[str, object]],
) -> List[str]:
    image_tags: List[str] = []
    blips = run.element.xpath(".//a:blip")
    for blip in blips:
        rel_id = blip.get(f"{{{_REL_NS}}}embed")
        if not rel_id:
            continue
        if image_stats is not None:
            rel_ids = image_stats.setdefault("rel_ids", set())
            if rel_id not in rel_ids:
                rel_ids.add(rel_id)
                image_stats["found"] = int(image_stats.get("found", 0)) + 1
        cached = image_cache.get(rel_id)
        if cached:
            asset_id, file_name = cached
        else:
            part = document.part.related_parts.get(rel_id)
            if not part:
                continue
            file_name = Path(str(part.partname)).name
            asset_id = asset_uploader(file_name or None, part.content_type, part.blob)
            image_cache[rel_id] = (asset_id, file_name)
            if image_stats is not None:
                image_stats["uploaded"] = int(image_stats.get("uploaded", 0)) + 1
        alt_text = escape(file_name or "News image")
        image_tags.append(f'<img src="/news/assets/{asset_id}/" alt="{alt_text}">')
    return image_tags


def _paragraph_html(
    *,
    paragraph: Paragraph,
    document: Document,
    image_cache: Dict[str, Tuple[int, str]],
    asset_uploader: Callable[[str | None, str | None, bytes], int],
    heading_level: Optional[int],
    image_stats: Optional[Dict[str, object]],
) -> str:
    parts: List[str] = []
    for child in paragraph._p.iterchildren():
        tag = child.tag
        if tag.endswith("}hyperlink"):
            rel_id = child.get(f"{{{_HYPERLINK_NS}}}id")
            href = _resolve_hyperlink_target(document, rel_id)
            link_parts: List[str] = []
            for run_el in child.xpath(".//w:r"):
                run = Run(run_el, paragraph)
                text_html = _format_run_text(run)
                if text_html:
                    link_parts.append(text_html)
                link_parts.extend(
                    _extract_run_images(
                        run=run,
                        document=document,
                        image_cache=image_cache,
                        asset_uploader=asset_uploader,
                        image_stats=image_stats,
                    )
                )
            if not link_parts:
                continue
            link_content = "".join(link_parts)
            if href:
                parts.append(f'<a href="{escape(href, quote=True)}">{link_content}</a>')
            else:
                parts.append(link_content)
            continue
        if tag.endswith("}r"):
            run = Run(child, paragraph)
            text_html = _format_run_text(run)
            if text_html:
                parts.append(text_html)
            parts.extend(
                _extract_run_images(
                    run=run,
                    document=document,
                    image_cache=image_cache,
                    asset_uploader=asset_uploader,
                    image_stats=image_stats,
                )
            )
    content = "".join(parts).strip()
    if not content:
        return ""
    content = _auto_link_internal_company_label(paragraph.text or "", content)
    tag = "p"
    if heading_level is not None:
        if heading_level <= 1:
            tag = "h2"
        elif heading_level == 2:
            tag = "h3"
        else:
            tag = "h4"
    return f"<{tag}>{content}</{tag}>"


def _table_html(table: Table) -> str:
    rows_html: List[str] = []
    for row in table.rows:
        cells_html: List[str] = []
        for cell in row.cells:
            cell_text = escape(cell.text or "").replace("\n", "<br>")
            cells_html.append(f"<td>{cell_text}</td>")
        if cells_html:
            rows_html.append(f"<tr>{''.join(cells_html)}</tr>")
    if not rows_html:
        return ""
    return f"<table><tbody>{''.join(rows_html)}</tbody></table>"


def build_news_body_from_docx(
    path: str,
    *,
    asset_uploader: Callable[[str | None, str | None, bytes], int],
    stats: Optional[Dict[str, int]] = None,
) -> Tuple[str, str]:
    document = Document(path)
    headline: Optional[str] = None
    first_text: Optional[str] = None
    first_paragraph_html: Optional[str] = None
    image_cache: Dict[str, Tuple[int, str]] = {}
    image_stats: Optional[Dict[str, object]] = {"found": 0, "uploaded": 0} if stats is not None else None
    body_parts: List[str] = []

    for block in _iter_block_items(document):
        if isinstance(block, Paragraph):
            text = (block.text or "").strip()
            if first_text is None and text:
                first_text = text
            level = _heading_level(block)
            if headline is None and text and level == 1:
                headline = text
                continue
            paragraph_html = _paragraph_html(
                paragraph=block,
                document=document,
                image_cache=image_cache,
                asset_uploader=asset_uploader,
                heading_level=level,
                image_stats=image_stats,
            )
            if first_paragraph_html is None and paragraph_html:
                first_paragraph_html = paragraph_html
            if paragraph_html:
                body_parts.append(paragraph_html)
        else:
            table_html = _table_html(block)
            if table_html:
                body_parts.append(table_html)

    if headline is None:
        headline = first_text or ""
        if first_paragraph_html and body_parts[:1] == [first_paragraph_html]:
            body_parts = body_parts[1:]

    body_html = "".join(body_parts).strip()
    if stats is not None and image_stats is not None:
        stats["images_found"] = int(image_stats.get("found", 0))
        stats["images_uploaded"] = int(image_stats.get("uploaded", 0))
    return headline, body_html
