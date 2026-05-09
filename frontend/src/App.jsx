import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertTriangle,
  BarChart3,
  CheckCircle2,
  Download,
  FileText,
  Loader2,
  MessageSquare,
  RefreshCw,
  Send,
  ShieldCheck,
  Sparkles,
  TableProperties,
  Trash2,
  UploadCloud,
  Users,
} from 'lucide-react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { absoluteApiUrl, analyzePdf, deleteSession, getHealth, getSession, listSessions, sendConsultantMessage } from './api';

const DEFAULT_KPIS = [
  { name: 'Scope 1', value: null, unit: 'tCO2e' },
  { name: 'Scope 2', value: null, unit: 'tCO2e' },
  { name: 'Water', value: null, unit: 'kL' },
  { name: 'Waste generated', value: null, unit: 'tonnes' },
  { name: 'Waste recycled', value: null, unit: 'tonnes' },
];

const ANALYSIS_STEPS = [
  'Upload',
  'Parse PDF',
  'Extract KPIs',
  'Cluster',
  'Forecast',
  'Report',
];

export default function App() {
  const [health, setHealth] = useState(null);
  const [sessions, setSessions] = useState([]);
  const [selectedFile, setSelectedFile] = useState(null);
  const [activeThread, setActiveThread] = useState(null);
  const [result, setResult] = useState(null);
  const [busy, setBusy] = useState(false);
  const [runningAnalysis, setRunningAnalysis] = useState(false);
  const [activeStep, setActiveStep] = useState(0);
  const [forecastZoom, setForecastZoom] = useState(null);
  const [benchmarkZoom, setBenchmarkZoom] = useState(null);
  const [trendZoom, setTrendZoom] = useState(null);
  const [chatMessages, setChatMessages] = useState([]);
  const [chatDraft, setChatDraft] = useState('');
  const [chatBusy, setChatBusy] = useState(false);
  const [error, setError] = useState('');
  const fileInputRef = useRef(null);

  useEffect(() => {
    refreshSystem();
  }, []);

  useEffect(() => {
    if (!runningAnalysis) return undefined;
    const timer = window.setInterval(() => {
      setActiveStep((step) => Math.min(step + 1, ANALYSIS_STEPS.length - 1));
    }, 4500);
    return () => window.clearInterval(timer);
  }, [runningAnalysis]);

  useEffect(() => {
    setForecastZoom(null);
    setBenchmarkZoom(null);
    setTrendZoom(null);
  }, [result?.session_id]);

  async function refreshSystem() {
    const [healthPayload, sessionPayload] = await Promise.all([
      getHealth().catch(() => null),
      listSessions().catch(() => []),
    ]);
    setHealth(healthPayload);
    setSessions(sessionPayload);
  }

  function handleFileSelect(file) {
    setSelectedFile(file);
    setActiveThread(file ? { filename: file.name, session_id: null } : null);
    setResult(null);
    setChatMessages([]);
    setChatDraft('');
    setActiveStep(0);
    setError('');
  }

  async function handleSubmit(event) {
    event.preventDefault();
    if (!selectedFile) return;

    const uploadName = selectedFile.name;
    setBusy(true);
    setRunningAnalysis(true);
    setActiveStep(0);
    setActiveThread({ filename: uploadName, session_id: null });
    setResult(null);
    setChatMessages([]);
    setChatDraft('');
    setError('');
    try {
      const payload = await analyzePdf({
        file: selectedFile,
      });
      setResult(payload);
      setActiveThread({ filename: uploadName, session_id: payload.session_id });
      setSelectedFile(null);
      if (fileInputRef.current) fileInputRef.current.value = '';
      setChatMessages([]);
      setActiveStep(ANALYSIS_STEPS.length);
      await refreshSystem();
    } catch (err) {
      setError(err.message);
    } finally {
      setRunningAnalysis(false);
      setBusy(false);
    }
  }

  async function loadThread(sessionId) {
    setBusy(true);
    setResult(null);
    setSelectedFile(null);
    setChatMessages([]);
    setChatDraft('');
    setError('');
    if (fileInputRef.current) fileInputRef.current.value = '';
    try {
      const thread = await getSession(sessionId);
      setResult(thread.result || null);
      setActiveThread({ filename: thread.filename, session_id: thread.session_id });
      setChatMessages(thread.chat_messages || []);
      setActiveStep(thread.result ? ANALYSIS_STEPS.length : 0);
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  }

  async function handleDeleteSession(event, sessionId) {
    event.stopPropagation();
    setError('');
    try {
      await deleteSession(sessionId);
      if (result?.session_id === sessionId || activeThread?.session_id === sessionId) {
        setResult(null);
        setActiveThread(null);
        setChatMessages([]);
        setChatDraft('');
      }
      await refreshSystem();
    } catch (err) {
      setError(err.message);
    }
  }

  async function handleChatSubmit(event) {
    event.preventDefault();
    if (!result?.session_id || !chatDraft.trim() || chatBusy) return;

    const message = chatDraft.trim();
    setChatDraft('');
    setChatBusy(true);
    setError('');
    setChatMessages((messages) => [
      ...messages,
      { role: 'user', content: message, at: new Date().toISOString() },
    ]);

    try {
      const payload = await sendConsultantMessage({
        sessionId: result.session_id,
        message,
      });
      setChatMessages(payload.messages || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setChatBusy(false);
    }
  }

  const forecastRows = useMemo(() => {
    const rows = result?.charts?.emissions_forecast || [];
    return normalizeChartRows(rows);
  }, [result]);

  const kpiRows = useMemo(() => {
    const rows = result?.charts?.kpi_snapshot || [];
    return rows.length ? rows : DEFAULT_KPIS;
  }, [result]);

  const benchmarkRows = useMemo(() => {
    const rows = result?.charts?.peer_benchmark || [];
    return normalizeChartRows(rows);
  }, [result]);

  const trendRows = useMemo(() => {
    const rows = result?.charts?.kpi_trends?.length
      ? result.charts.kpi_trends
      : recordsToTrendRows(result?.extracted_kpis?.yearly_records || []);
    return normalizeTrendRows(rows);
  }, [result]);

  const visibleForecastRows = useMemo(() => sliceByZoom(forecastRows, forecastZoom), [forecastRows, forecastZoom]);
  const visibleBenchmarkRows = useMemo(() => sliceByZoom(benchmarkRows, benchmarkZoom), [benchmarkRows, benchmarkZoom]);
  const visibleTrendRows = useMemo(() => sliceByZoom(trendRows, trendZoom), [trendRows, trendZoom]);

  const companyName = result?.extracted_kpis?.company_name || 'Pending company';
  const sourceName = selectedFile?.name || activeThread?.filename || result?.source_pdf_id || 'No PDF selected';
  const commandTitle = selectedFile?.name || activeThread?.filename || 'Upload an ESG/BRSR PDF';
  const commandSubtitle = selectedFile
    ? 'Ready to start a new analysis thread'
    : result
      ? 'Thread ready'
      : 'Extraction, clustering, peer comparison, forecast';
  const quality = result?.extraction_quality?.level || 'pending';
  const clusterLabel = result?.cluster?.KMeans_cluster_label || 'Awaiting cluster';
  const kpiIsEmpty = result && kpiRows.every((row) => row.value === null || row.value === undefined);
  const hasForecastChart = Boolean(result && forecastRows.length);
  const hasBenchmarkChart = Boolean(result && benchmarkRows.length);
  const hasTrendChart = Boolean(result && trendRows.length > 1);
  const hasYearlyKpis = Boolean(result && trendRows.length);
  const yearsLabel = trendRows.length
    ? trendRows.map((row) => row.year).filter(Boolean).join(', ')
    : result?.detected_years?.join(', ') || 'pending';

  return (
    <main className="app-canvas">
      <section className="product-shell">
        <aside className="left-panel">
          <div className="panel-brand">
            <div className="brand-mark">E</div>
            <div>
              <strong>ESG Intelligence</strong>
              <span>Analysis threads</span>
            </div>
          </div>

          <section className="side-section">
            <div className="side-title">
              <span>Recent Analyses</span>
              <button className="mini-icon" onClick={refreshSystem} aria-label="Refresh analyses">
                <RefreshCw size={14} />
              </button>
            </div>
            <div className="recent-list">
              {sessions.length === 0 && <p className="quiet">No threads yet</p>}
              {sessions.slice(0, 7).map((session) => (
                <div key={session.session_id} className="recent-item">
                  <button className="recent-open" onClick={() => loadThread(session.session_id)}>
                    <span>{session.company_name || session.filename}</span>
                    <small>{session.quality_level || 'open'}</small>
                  </button>
                  <button
                    className="delete-session"
                    onClick={(event) => handleDeleteSession(event, session.session_id)}
                    aria-label="Delete session"
                  >
                    <Trash2 size={13} />
                  </button>
                </div>
              ))}
            </div>
          </section>

          <section className="side-section system-card">
            <div className="side-title">
              <span>System</span>
            </div>
            <StatusRow label="OpenRouter" ok={health?.openrouter_configured} />
            <StatusRow label="CSV" ok={health?.csv_database_ready} />
            <StatusRow label="Models" ok={health?.model_paths_ready} />
            <StatusRow label="Parser" ok={Boolean(health)} value={health?.parser_mode || 'offline'} />
          </section>

          <button className="upload-side-button" onClick={() => fileInputRef.current?.click()}>
            <UploadCloud size={15} />
            <span>Upload ESG PDF</span>
          </button>
        </aside>

        <section className="main-panel">
          <header className="top-bar">
            <div className="workspace-label">
              <span>Workspace</span>
              <strong>ESG Risk & Forecast Analysis</strong>
            </div>
            <div className="top-actions">
              <div className="view-button">
                <ShieldCheck size={15} />
                <span>{quality}</span>
              </div>
              {result && (
                <a className="save-report" href={absoluteApiUrl(result.downloads.pdf)}>
                  <Download size={15} />
                  <span>Save Report</span>
                </a>
              )}
            </div>
          </header>

          <form className="analysis-command" onSubmit={handleSubmit}>
            <input
              ref={fileInputRef}
              type="file"
              accept="application/pdf"
              className="hidden-input"
              onChange={(event) => handleFileSelect(event.target.files?.[0] || null)}
            />
            <button type="button" className="upload-token" onClick={() => fileInputRef.current?.click()}>
              <UploadCloud size={16} />
            </button>
            <div className="command-copy">
              <strong>{commandTitle}</strong>
              <span>{commandSubtitle}</span>
            </div>
            <button className="run-button" disabled={!selectedFile || busy}>
              {busy ? <Loader2 className="spin" size={18} /> : <Sparkles size={18} />}
            </button>
          </form>

          <PipelineStatus
            steps={ANALYSIS_STEPS}
            activeStep={activeStep}
            running={runningAnalysis}
            complete={Boolean(result) && !runningAnalysis}
            error={Boolean(error)}
          />

          {error && (
            <div className="error-banner">
              <AlertTriangle size={18} />
              <span>{error}</span>
            </div>
          )}

          <div className="filter-row">
              <Chip label="Source" value={sourceName} />
              <Chip label="Metric" value="Scope 1 + 2" />
              <Chip label="KMeans cluster" value={result?.cluster?.KMeans_cluster ?? 'pending'} />
              <Chip label="Years" value={yearsLabel} />
              <Chip label="Company" value={companyName} />
          </div>

          <section className="content-grid">
            <EmissionsForecastCard
              hasData={hasForecastChart}
              rows={visibleForecastRows}
              rawRowCount={forecastRows.length}
              zoom={forecastZoom}
              setZoom={setForecastZoom}
              result={result}
            />
            <ExtractedKpiPreviewCard
              hasYearlyKpis={hasYearlyKpis}
              trendRows={trendRows}
              kpiRows={kpiRows}
              kpiIsEmpty={kpiIsEmpty}
              usedCsvFallback={Boolean(result?.extracted_kpis?.evidence?.csv_fallback)}
            />
            <ClusterPeerBenchmarkCard
              hasData={hasBenchmarkChart}
              rows={visibleBenchmarkRows}
              rawRowCount={benchmarkRows.length}
              zoom={benchmarkZoom}
              setZoom={setBenchmarkZoom}
              clusterId={result?.cluster?.KMeans_cluster}
            />
            <YearlyKpiTrendsCard
              hasData={hasTrendChart}
              rows={visibleTrendRows}
              rawRowCount={trendRows.length}
              zoom={trendZoom}
              setZoom={setTrendZoom}
              yearCount={trendRows.length}
            />
          </section>
        </section>

        <aside className="thread-panel">
          <div className="thread-header">
            <div>
              <span>Conversation Thread</span>
              <strong>{companyName}</strong>
            </div>
            <MessageSquare size={17} />
          </div>

          <div className="thread-scroll">
            <div className="thread-date">Today</div>
            <ThreadEvent
              icon={<FileText size={16} />}
              title={sourceName}
              body={result ? `${result.selected_pages.length} KPI pages, years: ${yearsLabel}.` : 'Waiting for upload.'}
            />
            <div className="user-bubble">
              Analyze ESG performance across all available years, assign cluster peers, forecast emissions, and prepare consultant notes.
            </div>
            <div className="assistant-bubble">
              <div className="assistant-label">
                <Sparkles size={14} />
                <span>Consultant</span>
              </div>
              <p>{result?.consultant_report?.executive_summary || 'Ready to generate the analysis thread.'}</p>
              <div className="mini-report">
                <strong>{clusterLabel}</strong>
                <span>Quality: {quality}</span>
                {result && (
                  <a href={absoluteApiUrl(result.downloads.html)} target="_blank" rel="noreferrer">
                    Open report
                  </a>
                )}
              </div>
            </div>
            {result && (
              <>
                <ThreadNote title="Risks" items={result.consultant_report.risks} />
                <ThreadNote title="Recommendations" items={result.consultant_report.recommendations} />
              </>
            )}
            {chatMessages.map((message, index) => (
              <div className={message.role === 'user' ? 'user-bubble' : 'assistant-bubble'} key={`${message.at}-${index}`}>
                {message.role === 'assistant' && (
                  <div className="assistant-label">
                    <Sparkles size={14} />
                    <span>Consultant</span>
                  </div>
                )}
                <p>{message.content}</p>
              </div>
            ))}
          </div>
          <form className="chat-form" onSubmit={handleChatSubmit}>
            <input
              value={chatDraft}
              onChange={(event) => setChatDraft(event.target.value)}
              disabled={!result || chatBusy}
              placeholder={result ? 'Ask the ESG consultant...' : 'Generate a report first'}
            />
            <button disabled={!result || !chatDraft.trim() || chatBusy} aria-label="Send message">
              {chatBusy ? <Loader2 className="spin" size={15} /> : <Send size={15} />}
            </button>
          </form>
        </aside>
      </section>
    </main>
  );
}

function StatusRow({ label, ok, value }) {
  return (
    <div className="status-row">
      <span>{label}</span>
      <strong className={ok ? 'ok' : 'bad'}>{value || (ok ? 'ready' : 'missing')}</strong>
    </div>
  );
}

function Chip({ label, value }) {
  return (
    <div className="chip">
      <span>{label}:</span>
      <strong>{value}</strong>
    </div>
  );
}

function PipelineStatus({ steps, activeStep, running, complete, error }) {
  return (
    <section className="pipeline-status">
      {steps.map((step, index) => {
        const isDone = complete || activeStep > index;
        const isActive = running && activeStep === index;
        return (
          <div className={`pipeline-step ${isDone ? 'done' : ''} ${isActive ? 'active' : ''} ${error ? 'error' : ''}`} key={step}>
            <div className="step-dot">
              {isDone ? <CheckCircle2 size={14} /> : isActive ? <Loader2 className="spin" size={13} /> : index + 1}
            </div>
            <span>{step}</span>
          </div>
        );
      })}
    </section>
  );
}

function CardHeader({ icon, title, action }) {
  return (
    <div className="card-header">
      <div>
        {icon}
        <h2>{title}</h2>
      </div>
      <button>{action}</button>
    </div>
  );
}

function EmissionsForecastCard({ hasData, rows, rawRowCount, zoom, setZoom, result }) {
  return (
    <article className="chart-card primary-chart">
      <CardHeader
        icon={<BarChart3 size={16} />}
        title="Emissions Forecast vs Cluster Peers"
        action={result ? 'Model output' : 'Preview'}
      />
      <div
        className="chart-box zoomable-chart"
        onWheel={(event) => handleWheelZoom(event, rawRowCount, zoom, setZoom)}
        onDoubleClick={() => setZoom(null)}
      >
        {hasData ? (
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={rows}>
              <CartesianGrid stroke="#edf0f2" vertical={false} />
              <XAxis dataKey="year" axisLine={false} tickLine={false} />
              <YAxis axisLine={false} tickLine={false} width={58} />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="company" stroke="#15616d" strokeWidth={3} dot={{ r: 3 }} connectNulls />
              <Line type="monotone" dataKey="peer" stroke="#7c3aed" strokeWidth={2} dot={{ r: 3 }} connectNulls />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <ChartEmpty title="No forecast yet" body="Upload and analyze a PDF to generate model-backed chart data." />
        )}
      </div>
    </article>
  );
}

function ExtractedKpiPreviewCard({ hasYearlyKpis, trendRows, kpiRows, kpiIsEmpty, usedCsvFallback }) {
  return (
    <article className="chart-card kpi-card">
      <CardHeader
        icon={<TableProperties size={16} />}
        title="Extracted KPI Preview"
        action={hasYearlyKpis ? `${trendRows.length} year${trendRows.length === 1 ? '' : 's'}` : 'Latest'}
      />
      {hasYearlyKpis ? (
        <div className="yearly-kpi-table">
          <div className="yearly-row yearly-head">
            <span>Year</span>
            <span>Scope 1</span>
            <span>Scope 2</span>
            <span>Total</span>
            <span>Water</span>
            <span>Waste gen.</span>
            <span>Recycled</span>
          </div>
          {trendRows.map((row) => (
            <div className="yearly-row" key={row.year}>
              <strong>{row.year || '-'}</strong>
              <span>{formatNumber(row.scope1)}</span>
              <span>{formatNumber(row.scope2)}</span>
              <span>{formatNumber(row.total_emissions)}</span>
              <span>{formatNumber(row.water)}</span>
              <span>{formatNumber(row.waste_generated)}</span>
              <span>{formatNumber(row.waste_recycled)}</span>
            </div>
          ))}
        </div>
      ) : (
        <div className="data-table">
          <div className="table-row table-head">
            <span>Metric</span>
            <span>Value</span>
            <span>Unit</span>
          </div>
          {kpiRows.map((row) => (
            <div className="table-row" key={row.name}>
              <span>{row.name}</span>
              <strong>{formatNumber(row.value)}</strong>
              <small>{row.unit}</small>
            </div>
          ))}
        </div>
      )}
      {kpiIsEmpty && (
        <div className="kpi-empty-note">
          Numeric KPI values were not found in the extracted context. Try the old OCR/table parser for this PDF.
        </div>
      )}
      {usedCsvFallback && (
        <div className="kpi-source-note">
          Missing values filled from CSV company match.
        </div>
      )}
    </article>
  );
}

function ClusterPeerBenchmarkCard({ hasData, rows, rawRowCount, zoom, setZoom, clusterId }) {
  return (
    <article className="chart-card benchmark-card">
      <CardHeader icon={<Users size={16} />} title="Cluster Peer Benchmark" action={`Cluster ${clusterId ?? '-'}`} />
      <div
        className="chart-box small zoomable-chart"
        onWheel={(event) => handleWheelZoom(event, rawRowCount, zoom, setZoom)}
        onDoubleClick={() => setZoom(null)}
      >
        {hasData ? (
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={rows}>
              <CartesianGrid stroke="#edf0f2" vertical={false} />
              <XAxis dataKey="metric" axisLine={false} tickLine={false} />
              <YAxis axisLine={false} tickLine={false} width={58} />
              <Tooltip />
              <Legend />
              <Bar dataKey="company" fill="#15616d" radius={[4, 4, 0, 0]} />
              <Bar dataKey="peer" fill="#f0b429" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <ChartEmpty title="No benchmark yet" body="Cluster peers appear after the analysis completes." />
        )}
      </div>
    </article>
  );
}

function YearlyKpiTrendsCard({ hasData, rows, rawRowCount, zoom, setZoom, yearCount }) {
  return (
    <article className="chart-card trend-card">
      <CardHeader icon={<BarChart3 size={16} />} title="Yearly KPI Trends" action={hasData ? `${yearCount} years` : 'Need 2 years'} />
      <div
        className="chart-box small zoomable-chart"
        onWheel={(event) => handleWheelZoom(event, rawRowCount, zoom, setZoom)}
        onDoubleClick={() => setZoom(null)}
      >
        {hasData ? (
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={rows}>
              <CartesianGrid stroke="#edf0f2" vertical={false} />
              <XAxis dataKey="year" axisLine={false} tickLine={false} />
              <YAxis axisLine={false} tickLine={false} width={58} />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="total_emissions" name="Total emissions" stroke="#15616d" strokeWidth={3} connectNulls />
              <Line type="monotone" dataKey="water" name="Water" stroke="#0f766e" strokeWidth={2} connectNulls />
              <Line type="monotone" dataKey="waste_generated" name="Waste generated" stroke="#d97706" strokeWidth={2} connectNulls />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <ChartEmpty title="No multi-year trend yet" body="If the PDF has two or more extracted years, this chart adapts automatically." />
        )}
      </div>
    </article>
  );
}

function ChartEmpty({ title, body }) {
  return (
    <div className="chart-empty">
      <BarChart3 size={24} />
      <strong>{title}</strong>
      <span>{body}</span>
    </div>
  );
}

function ThreadEvent({ icon, title, body }) {
  return (
    <div className="thread-event">
      <div className="event-icon">{icon}</div>
      <div>
        <strong>{title}</strong>
        <span>{body}</span>
      </div>
    </div>
  );
}

function ThreadNote({ title, items }) {
  const safeItems = Array.isArray(items) ? items : [];
  return (
    <div className="thread-note">
      <strong>{title}</strong>
      {safeItems.slice(0, 3).map((item) => (
        <span key={item}>{item}</span>
      ))}
    </div>
  );
}

function normalizeChartRows(rows) {
  return rows.map((row) => {
    const next = { ...row };
    for (const key of ['company', 'peer']) {
      if (next[key] === null || next[key] === undefined || next[key] === '') {
        next[key] = undefined;
      } else {
        next[key] = Number(next[key]);
      }
    }
    return next;
  });
}

function recordsToTrendRows(records) {
  return records.map((record) => ({
    year: record.fiscal_year_start,
    scope1: record.scope1_tco2e,
    scope2: record.scope2_tco2e,
    total_emissions: record.total_scope1_scope2_tco2e ?? record.computed_total_scope1_scope2_tco2e,
    water: record.water_consumption_kl,
    waste_generated: record.waste_generated_tonnes,
    waste_recycled: record.waste_recycled_tonnes,
  }));
}

function normalizeTrendRows(rows) {
  return rows.map((row) => {
    const next = { ...row };
    for (const key of ['scope1', 'scope2', 'total_emissions', 'water', 'waste_generated', 'waste_recycled']) {
      if (next[key] === null || next[key] === undefined || next[key] === '') {
        next[key] = undefined;
      } else {
        next[key] = Number(next[key]);
      }
    }
    return next;
  });
}

function sliceByZoom(rows, zoom) {
  if (!zoom || !rows.length) return rows;
  return rows.slice(zoom.start, zoom.end + 1);
}

function handleWheelZoom(event, rowCount, zoom, setZoom) {
  if (rowCount <= 2) return;

  event.preventDefault();
  const bounds = event.currentTarget.getBoundingClientRect();
  const centerRatio = clamp((event.clientX - bounds.left) / Math.max(bounds.width, 1), 0, 1);
  const currentStart = zoom?.start ?? 0;
  const currentEnd = zoom?.end ?? rowCount - 1;
  const currentSize = currentEnd - currentStart + 1;
  const zoomingIn = event.deltaY < 0;
  const step = Math.max(1, Math.round(currentSize * 0.22));
  const nextSize = zoomingIn
    ? Math.max(2, currentSize - step)
    : Math.min(rowCount, currentSize + step);

  if (nextSize >= rowCount) {
    setZoom(null);
    return;
  }

  const centerIndex = currentStart + Math.round((currentSize - 1) * centerRatio);
  let nextStart = centerIndex - Math.round((nextSize - 1) * centerRatio);
  nextStart = clamp(nextStart, 0, rowCount - nextSize);
  setZoom({ start: nextStart, end: nextStart + nextSize - 1 });
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function formatNumber(value) {
  if (value === null || value === undefined) return '-';
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 });
}
