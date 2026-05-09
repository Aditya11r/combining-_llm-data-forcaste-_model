# ESG PDF Intelligence System

Full-stack scaffold for ESG/BRSR PDF analysis:

1. Upload a PDF.
2. Build a KPI-focused context from the PDF.
3. Extract structured ESG fields with an OpenRouter LLM.
4. Run clustering from the saved preprocessing/PCA/KMeans artifacts.
5. Use `peer_group` only as a redundant alias of `KMeans_cluster`.
6. Run emissions forecasting from the saved Keras model.
7. Compare against the current CSV database.
8. Generate a consultant-style report and frontend chart payloads.
9. Save the complete session thread with context, extraction, model outputs, and report.

## Where To Set OpenRouter Models

Copy `backend/.env.example` to `backend/.env`, then edit:

```env
OPENROUTER_API_KEY=your_openrouter_key
OPENROUTER_EXTRACTION_MODEL=openai/gpt-4o-mini
OPENROUTER_REPORT_MODEL=openai/gpt-4o
```

Use any OpenRouter model id there. The extraction model should be strong at structured JSON. The report model can be a better reasoning/writing model.

## Asset Paths

The default `.env.example` points to your current model and CSV files:

```env
KMEANS_MODEL_PATH=C:\Users\adiko\Downloads\intern_3_model\intern_3\models\kmeans_model.pkl
PREPROCESSOR_PATH=C:\Users\adiko\Downloads\intern_3_model\intern_3\models\preprocessor.pkl
PCA_PATH=C:\Users\adiko\Downloads\intern_3_model\intern_3\models\pca.pkl
LSTM_MODEL_PATH=C:\Users\adiko\Downloads\intern_3_model\intern_3\models\lstm_model.keras
LSTM_SCALER_PATH=C:\Users\adiko\Downloads\intern_3_model\intern_3\models\lstm_scaler.pkl
CLUSTER_SUMMARY_CSV=C:\Users\adiko\Downloads\intern_3_model\intern_3\data\cluster_summary.csv
PEER_GROUPS_CSV=C:\Users\adiko\Downloads\intern_3_model\intern_3\data\peer_groups.csv
CLUSTER_FORECAST_CSV=C:\Users\adiko\Downloads\intern_3_model\intern_3\data\cluster_forecast.csv
```

## Old Parser Integration

When your old parser is available, expose a function named `prepare_pdf_context(path: Path)` that returns an object with:

```python
pdf_path
source_pdf_id
context
selected_pages
detected_years
target_years
```

Then set:

```env
OLD_PARSER_MODULE=your_package.your_module
OLD_PARSER_FUNCTION=prepare_pdf_context
```

Until that is set, the backend uses a basic `pypdf` fallback parser.

## Run Backend

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
uvicorn app.main:app --reload --port 8000
```

On this machine, `python` was not available in the current shell when this scaffold was created. Install Python or add it to PATH before running the backend.

## Run Frontend

```powershell
cd frontend
npm.cmd install
npm.cmd run dev
```

The frontend expects the backend at `http://localhost:8000`. You can change that with:

```env
VITE_API_BASE_URL=http://localhost:8000
```

## API

- `POST /api/analyze-pdf`
- `GET /api/sessions`
- `GET /api/sessions/{session_id}`
- `GET /api/sessions/{session_id}/report.html`
- `GET /api/sessions/{session_id}/report.pdf`

The PDF report is a lightweight text PDF generated without external rendering dependencies. The HTML report is richer and better for the frontend/browser.

## Cluster And Peer Set Rule

`KMeans_cluster` is the authoritative model output. The old/current CSV also contains `peer_group`, but the backend treats that as a redundant compatibility field. Peer companies are selected by the predicted `KMeans_cluster`, and the same id is passed forward wherever legacy code expects `peer_group`.
