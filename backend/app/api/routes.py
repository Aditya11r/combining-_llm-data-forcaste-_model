from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, Response

from app.config import get_settings
from app.data.peer_store import PeerStore
from app.extraction.kpi_extractor import KpiExtractor, score_extraction
from app.models.clustering import ClusteringService
from app.models.forecasting import ForecastingService
from app.parser.pdf_context import prepare_pdf_context
from app.report.chart_builder import build_charts
from app.report.chat import ConsultantChatService, make_chat_message
from app.report.consultant import ConsultantReporter
from app.report.exporter import build_report_html, build_simple_pdf
from app.schemas import AnalysisResponse, ChatRequest, ChatResponse, HealthResponse, YearlyKpiRecord
from app.sessions.store import SessionStore

router = APIRouter()


NUMERIC_KPI_FIELDS = [
    "scope1_tco2e",
    "scope2_tco2e",
    "water_consumption_kl",
    "waste_generated_tonnes",
    "waste_recycled_tonnes",
    "total_scope1_scope2_tco2e",
]


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    peer_store = PeerStore(settings)
    model_paths_ready = all(
        path.exists()
        for path in [
            settings.kmeans_model_path,
            settings.preprocessor_path,
            settings.pca_path,
            settings.lstm_model_path,
            settings.lstm_scaler_path,
        ]
    )
    return HealthResponse(
        status="ok",
        openrouter_configured=bool(settings.openrouter_api_key),
        parser_mode="old_parser" if settings.old_parser_module else "pypdf_fallback",
        csv_database_ready=peer_store.ready(),
        model_paths_ready=model_paths_ready,
    )


@router.post("/analyze-pdf", response_model=AnalysisResponse)
async def analyze_pdf(
    file: Annotated[UploadFile, File()],
    extraction_model: Annotated[str | None, Form()] = None,
    report_model: Annotated[str | None, Form()] = None,
) -> AnalysisResponse:
    settings = get_settings()
    store = SessionStore(settings)
    session = store.create(file.filename or "uploaded.pdf")

    if file.content_type not in {"application/pdf", "application/octet-stream"}:
        raise HTTPException(status_code=400, detail="Upload must be a PDF")

    upload_path = settings.upload_dir / f"{session['session_id']}_{safe_filename(file.filename or 'report.pdf')}"
    contents = await file.read()
    max_bytes = settings.max_upload_mb * 1024 * 1024
    if len(contents) > max_bytes:
        raise HTTPException(status_code=413, detail=f"PDF exceeds {settings.max_upload_mb} MB")

    upload_path.write_bytes(contents)
    session["artifacts"]["uploaded_pdf"] = str(upload_path)
    store.append_event(session, "pdf_uploaded", {"path": str(upload_path), "bytes": len(contents)})

    try:
        prepared = prepare_pdf_context(upload_path, settings)
        session["context"] = {
            "source_pdf_id": prepared.source_pdf_id,
            "selected_pages": prepared.selected_pages,
            "detected_years": prepared.detected_years,
            "target_years": prepared.target_years,
            "text": prepared.context,
        }
        store.append_event(
            session,
            "pdf_context_prepared",
            {
                "source_pdf_id": prepared.source_pdf_id,
                "selected_pages": prepared.selected_pages,
                "detected_years": prepared.detected_years,
                "target_years": prepared.target_years,
            },
        )

        extractor = KpiExtractor(settings)
        extracted, quality, raw_extraction = await extractor.extract(
            context=prepared.context,
            detected_years=prepared.detected_years,
            target_years=prepared.target_years,
            model_override=extraction_model,
        )
        store.append_event(session, "kpis_extracted", {"raw": raw_extraction, "validated": extracted.model_dump()})

        peer_store = PeerStore(settings)
        csv_records = peer_store.company_records(extracted.company_name)
        filled_fields = _fill_extracted_from_csv_records(extracted, csv_records, prepared.target_years)
        if filled_fields:
            extracted.warnings.append("Some KPI values were filled from CSV fallback because PDF extraction missed them.")
            extracted.evidence["csv_fallback"] = "cluster_forecast.csv company-year match"
            quality = score_extraction(extracted)
            quality.notes.append(
                "Filled missing KPI fields from the current CSV database across available years: "
                + ", ".join(sorted(filled_fields))
            )

        clustering = ClusteringService(settings, peer_store)
        try:
            cluster = clustering.predict(extracted.clustering_input)
        except Exception as exc:
            cluster = clustering.fallback_from_peer_group(0)
            quality.notes.append(f"Clustering model fallback used: {exc}")

        forecasting_inputs = extracted.to_forecasting_inputs(peer_group=cluster.peer_group)
        peer_comparison = peer_store.peer_comparison(cluster.peer_group)
        try:
            forecast = ForecastingService(settings).forecast(forecasting_inputs)
        except Exception as exc:
            forecast = []
            quality.notes.append(f"Forecasting skipped: {exc}")

        charts = build_charts(extracted=extracted, forecast=forecast, peer_comparison=peer_comparison)

        consultant_report = await ConsultantReporter(settings).build(
            extracted=extracted,
            quality=quality,
            cluster=cluster,
            forecast=forecast,
            peer_comparison=peer_comparison,
            charts=charts,
            model_override=report_model,
        )

        result = AnalysisResponse(
            session_id=session["session_id"],
            created_at=datetime.fromisoformat(session["created_at"]),
            source_pdf_id=prepared.source_pdf_id,
            selected_pages=prepared.selected_pages,
            detected_years=prepared.detected_years,
            target_years=prepared.target_years,
            extracted_kpis=extracted,
            extraction_quality=quality,
            cluster=cluster,
            forecast=forecast,
            peer_comparison=peer_comparison,
            charts=charts,
            consultant_report=consultant_report,
            downloads={
                "html": f"/api/sessions/{session['session_id']}/report.html",
                "pdf": f"/api/sessions/{session['session_id']}/report.pdf",
            },
        )

        session["result"] = result.model_dump(mode="json")
        store.append_event(session, "analysis_completed", {"result": session["result"]})
        return result

    except HTTPException:
        raise
    except Exception as exc:
        store.append_event(session, "analysis_failed", {"error": str(exc)})
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sessions")
def list_sessions():
    return SessionStore(get_settings()).list()


@router.get("/sessions/{session_id}")
def get_session(session_id: str):
    try:
        return SessionStore(get_settings()).get(session_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Session not found") from exc


@router.delete("/sessions/{session_id}", status_code=204)
def delete_session(session_id: str):
    try:
        SessionStore(get_settings()).delete(session_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Session not found") from exc
    return Response(status_code=204)


@router.post("/sessions/{session_id}/chat", response_model=ChatResponse)
async def chat_with_consultant(session_id: str, request: ChatRequest) -> ChatResponse:
    store = SessionStore(get_settings())
    try:
        session = store.get(session_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Session not found") from exc

    message = request.message.strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    session.setdefault("chat_messages", []).append(make_chat_message("user", message))
    answer = await ConsultantChatService(get_settings()).answer(
        session=session,
        message=message,
        model_override=request.model,
    )
    session["chat_messages"].append(make_chat_message("assistant", answer))
    store.append_event(session, "consultant_chat", {"message": message, "answer": answer})

    return ChatResponse(
        session_id=session_id,
        answer=answer,
        messages=session["chat_messages"],
    )


@router.get("/sessions/{session_id}/report.html", response_class=HTMLResponse)
def report_html(session_id: str):
    result = _load_result(session_id)
    return HTMLResponse(build_report_html(result))


@router.get("/sessions/{session_id}/report.pdf")
def report_pdf(session_id: str):
    result = _load_result(session_id)
    pdf = build_simple_pdf(result)
    return Response(
        content=pdf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="esg-report-{session_id}.pdf"'},
    )


def _load_result(session_id: str) -> AnalysisResponse:
    try:
        session = SessionStore(get_settings()).get(session_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Session not found") from exc

    if "result" not in session:
        raise HTTPException(status_code=404, detail="Session has no completed analysis")

    return AnalysisResponse.model_validate(session["result"])


def safe_filename(filename: str) -> str:
    allowed = []
    for char in Path(filename).name:
        if char.isalnum() or char in {".", "-", "_"}:
            allowed.append(char)
        else:
            allowed.append("_")
    return "".join(allowed) or "report.pdf"


def _safe_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _safe_int(value: str | None) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _fill_extracted_from_csv_records(extracted, csv_records: list[dict[str, str]], target_years: list[str]) -> set[str]:
    if not csv_records:
        return set()

    target_year_starts = {_fiscal_year_start_from_label(year) for year in target_years}
    target_year_starts.discard(None)
    existing_years = {record.fiscal_year_start for record in extracted.yearly_records if record.fiscal_year_start}
    wanted_years = existing_years or target_year_starts
    if not wanted_years:
        wanted_years = {_safe_int(row.get("fiscal_year_start")) for row in csv_records if _safe_int(row.get("fiscal_year_start"))}

    rows_by_year = {
        _safe_int(row.get("fiscal_year_start")): row
        for row in csv_records
        if _safe_int(row.get("fiscal_year_start")) is not None
    }

    filled_fields: set[str] = set()
    for year in sorted(wanted_years):
        if year not in rows_by_year:
            continue
        record = _yearly_record_for(extracted, year)
        row = rows_by_year[year]

        if record.fiscal_year is None:
            record.fiscal_year = row.get("fiscal_year")

        for field in NUMERIC_KPI_FIELDS:
            if getattr(record, field) is None:
                value = _safe_float(row.get(field))
                if value is not None:
                    setattr(record, field, value)
                    filled_fields.add(f"yearly_records.{year}.{field}")

    latest_csv = max(
        [row for row in csv_records if _safe_int(row.get("fiscal_year_start")) is not None],
        key=lambda row: _safe_int(row.get("fiscal_year_start")) or 0,
        default=None,
    )
    if latest_csv:
        if extracted.fiscal_year is None:
            extracted.fiscal_year = latest_csv.get("fiscal_year")
        if extracted.fiscal_year_start is None:
            extracted.fiscal_year_start = _safe_int(latest_csv.get("fiscal_year_start"))

    if extracted.fiscal_year_start:
        latest = _yearly_record_for(extracted, extracted.fiscal_year_start)
        for field in NUMERIC_KPI_FIELDS:
            if getattr(extracted, field) is None and getattr(latest, field) is not None:
                setattr(extracted, field, getattr(latest, field))
                filled_fields.add(field)

    extracted.yearly_records = sorted(
        extracted.yearly_records,
        key=lambda record: record.fiscal_year_start or 0,
    )
    return filled_fields


def _yearly_record_for(extracted, fiscal_year_start: int):
    latest = None
    for record in extracted.yearly_records:
        if record.fiscal_year_start == fiscal_year_start:
            latest = record
            break

    if latest is None:
        latest = YearlyKpiRecord(
            fiscal_year=f"FY {fiscal_year_start}-{str(fiscal_year_start + 1)[-2:]}",
            fiscal_year_start=fiscal_year_start,
        )
        extracted.yearly_records.append(latest)

    return latest


def _fiscal_year_start_from_label(value: str | None) -> int | None:
    if not value:
        return None
    import re

    match = re.search(r"(20\d{2})", value)
    if not match:
        return None
    return int(match.group(1))
