from __future__ import annotations

import hashlib
import importlib
import re
from dataclasses import dataclass
from pathlib import Path

from app.config import Settings


@dataclass
class PreparedPdfContext:
    pdf_path: Path
    source_pdf_id: str
    context: str
    selected_pages: list[int]
    detected_years: list[str]
    target_years: list[str]


def compute_source_document_id(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"pdf_{digest.hexdigest()[:20]}"


def detect_fiscal_years_from_context(context: str) -> list[str]:
    years: set[str] = set()
    patterns = [
        r"FY\s*(20\d{2})\s*[-/–—]\s*(\d{2,4})",
        r"(20\d{2})\s*[-/–—]\s*(\d{2,4})",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, context, flags=re.IGNORECASE):
            start = match.group(1)
            end = match.group(2)
            if len(end) == 4:
                end = end[-2:]

            start_year = int(start)
            end_year = int(f"{start[:2]}{end}")
            if end_year == start_year + 1:
                years.add(f"FY {start}-{end}")

    return sorted(years, reverse=True)


TARGET_YEAR_KEYWORDS = (
    "revenue",
    "turnover",
    "scope 1",
    "scope 2",
    "scope1",
    "scope2",
    "emissions",
    "ghg",
    "greenhouse gas emissions",
    "water consumption",
    "water withdrawal",
    "waste generated",
    "waste recycled",
    "principle 6",
    "essential indicators",
    "energy consumed",
    "total electricity consumption",
    "total fuel consumption",
    "waste management",
)

PAGE_SCORE_KEYWORDS = {
    "scope 1": 8,
    "scope 2": 8,
    "scope1": 8,
    "scope2": 8,
    "greenhouse gas": 7,
    "ghg": 7,
    "water consumption": 7,
    "water withdrawal": 5,
    "waste generated": 7,
    "waste recycled": 7,
    "principle 6": 5,
    "essential indicators": 4,
    "energy consumed": 4,
    "total electricity consumption": 4,
    "total fuel consumption": 4,
    "business responsibility": 2,
}


def detect_target_fiscal_years_from_context(context: str) -> list[str]:
    lines = [line.strip() for line in context.splitlines() if line.strip()]
    years: set[str] = set()

    for index, line in enumerate(lines):
        window = " ".join(lines[max(0, index - 3) : min(len(lines), index + 4)]).lower()
        if not any(keyword in window for keyword in TARGET_YEAR_KEYWORDS):
            continue
        years.update(detect_fiscal_years_from_context(line))

    return sorted(years, reverse=True)


def _load_old_parser(settings: Settings):
    if not settings.old_parser_module:
        return None

    module = importlib.import_module(settings.old_parser_module)
    return getattr(module, settings.old_parser_function)


def _prepare_with_pypdf(path: Path) -> PreparedPdfContext:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError(
            "No old parser is configured and pypdf is not installed. "
            "Install backend requirements or set OLD_PARSER_MODULE."
        ) from exc

    reader = PdfReader(str(path))
    page_blocks: list[tuple[int, str, int]] = []

    for index, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        if text.strip():
            page_blocks.append((index, f"## PAGE {index}\n{text.strip()}", _score_page(text)))

    selected_pages = _select_relevant_pages(page_blocks, total_pages=len(reader.pages))
    context = _build_prioritized_context(page_blocks, selected_pages)
    if not context.strip():
        raise RuntimeError("No extractable text found in PDF. Configure the old OCR parser for scanned PDFs.")

    return PreparedPdfContext(
        pdf_path=path,
        source_pdf_id=compute_source_document_id(path),
        context=context,
        selected_pages=selected_pages,
        detected_years=detect_fiscal_years_from_context(context),
        target_years=detect_target_fiscal_years_from_context(context),
    )


def _score_page(text: str) -> int:
    lower = text.lower()
    return sum(weight for keyword, weight in PAGE_SCORE_KEYWORDS.items() if keyword in lower)


def _select_relevant_pages(page_blocks: list[tuple[int, str, int]], total_pages: int) -> list[int]:
    scored_pages = sorted(
        [(page_number, score) for page_number, _, score in page_blocks if score > 0],
        key=lambda item: item[1],
        reverse=True,
    )

    if not scored_pages:
        return list(range(1, min(total_pages, 8) + 1))

    selected: set[int] = set()
    for page_number, _ in scored_pages[:12]:
        for neighbor in (page_number - 1, page_number, page_number + 1):
            if 1 <= neighbor <= total_pages:
                selected.add(neighbor)

    return sorted(selected)[:18]


def _build_prioritized_context(page_blocks: list[tuple[int, str, int]], selected_pages: list[int]) -> str:
    blocks_by_page = {page_number: block for page_number, block, _ in page_blocks}
    selected_blocks = [blocks_by_page[page] for page in selected_pages if page in blocks_by_page]

    intro_blocks = [
        block
        for page_number, block, _ in page_blocks
        if page_number <= 6 and page_number not in selected_pages
    ]

    return "\n\n".join(selected_blocks + intro_blocks)


def prepare_pdf_context(path: Path, settings: Settings) -> PreparedPdfContext:
    old_parser = _load_old_parser(settings)
    if old_parser is not None:
        result = old_parser(path)
        return PreparedPdfContext(
            pdf_path=Path(result.pdf_path),
            source_pdf_id=str(result.source_pdf_id),
            context=str(result.context),
            selected_pages=list(result.selected_pages),
            detected_years=list(result.detected_years),
            target_years=list(result.target_years),
        )

    return _prepare_with_pypdf(path)
