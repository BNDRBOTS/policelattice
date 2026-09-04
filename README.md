# Police Lattice

Autonomous, zero-mock-data pipeline for police accountability data: live
external acquisition, best-in-class parsing, external validation, officer
anomaly detection with exact statistics, plain-language semantic output, an
immutable monthly chron-archive stored inside the Railway PostgreSQL
database, and a high-fidelity dashboard that renders historical months with
exact parity to the live view.

## Execution Mandate (enforced structurally)

- **No manual data drops, no static seeding, no simulated responses.** Every
  enabled source in `app/source_catalog.yaml` is a live external endpoint.
  Sources that require interactive/authenticated portals are explicitly
  `enabled: false` with a documented reason — never substituted or faked.
- **Local placeholders are fatal:** unconfigured sources skip visibly; the
  catalog contains no `example.com` endpoints (asserted by tests).
- **Operation sequence is absolute:** Search → Gather → Organize → Process →
  Verify → Synthesize, executed exactly once per run, in order, inside an
  audited `pipeline_runs` row (asserted by tests).

## Architecture

```
source_catalog.yaml            live endpoints + runtime discovery directives
        |
        v
1. SEARCH                      Socrata Discovery Catalog API + ArcGIS Hub search
        |                      resolve dataset IDs / service URLs live; results
        |                      persisted onto data_sources.config.discovered
        v
2. GATHER                      httpx pooled transport + orjson/lxml/pypdfium2/
        |                      pandas parsers; SHA-256 raw snapshots (dedup)
        v
3. ORGANIZE                    canonical normalization -> staging records with
        |                      full provenance (source, live_url, checksum)
        v
4. PROCESS                     rule-based evidence extraction (officers, force
        |                      taxonomy, ARS statutes, dockets, disclosures)
        v
5. VERIFY                      record checks (provenance / integrity / canonical
        |                      form / temporal / non-empty) + external
        |                      revalidation: sources re-fetched live and sampled
        |                      checksums confirmed present. Failures -> failed,
        |                      never synthesized.
        v
6. SYNTHESIZE                  lattice synthesis + dependency resolution +
        |                      month analytics + officer anomaly detection +
        |                      monthly chron-archive + retrieval index bump
        v
PostgreSQL lattice             agencies, officers, incidents, arrests, charges,
                              court_cases, documents, news, entity_links,
                              pipeline_runs, verification_results,
                              officer_anomaly_findings, monthly_archive_files
        |
        v
FastAPI + dashboard            month-parity analytics, larger pie charts,
                              full-fidelity tables, hybrid search
```

## Best-in-Class Component Selection (benchmark-validated)

| Concern | Component | Why |
|---|---|---|
| JSON parsing | `orjson` | Fastest RFC-compliant Python JSON (Rust core) |
| HTML parsing | `lxml` (via BeautifulSoup) | Fastest production engine (libxml2) |
| PDF text | `pypdfium2` | PDFium bindings; fastest permissive extractor |
| OCR | `tesseract` (`pytesseract`) | Benchmark-reference open-source OCR |
| RSS/Atom | `feedparser` | Reference-standard feed parser |
| Tabular | `pandas` | Benchmark-standard tabular engine |
| Lexical retrieval | `bm25s` | Lucene-grade BM25; order-of-magnitude faster than rank-bm25 |
| Semantic retrieval | `fastembed` (BAAI/bge-small-en-v1.5) | Top MTEB-small retrieval model, CPU/ONNX |
| Literal retrieval | `rapidfuzz` | Benchmark-standard C++ fuzzy matching |
| Fusion | Reciprocal Rank Fusion (k=60) | Standard tunable-free hybrid fusion |
| Statistics | `scipy` | Reference exact-test library (Poisson tails) |
| HTTP | `httpx` | Pooled, HTTP/2-capable, hard timeouts |

## Monthly Refresh Protocol & Immutable Chron-Logging

- The scheduler finalizes the just-ended month on the 1st of each month
  (02:00 America/Phoenix) and continuously re-archives the current month.
- Each month is persisted as **discrete files** inside the Railway database
  (`monthly_archive_files.payload`, BYTEA):
  `raw_records__YYYY-MM.jsonl.gz`, `staging_records__YYYY-MM.jsonl.gz`,
  `entities__YYYY-MM.jsonl.gz`, `analytics_snapshot__YYYY-MM.json.gz`,
  `anomaly_findings__YYYY-MM.jsonl.gz`.
- Files are content-addressed (SHA-256) and **append-only**:
  - identical content is never duplicated;
  - changed content appends a NEW versioned file (`-v2`, `-v3`, …), so the
    archive preserves the full history of what was known when;
  - PostgreSQL triggers physically reject `UPDATE`/`DELETE` on the archive
    tables (`app.db.install_immutability_guards`);
  - downloads verify the stored digest at read time (`/api/archive/file/{id}`
    returns 409 on any mismatch).
- `GET /api/analytics?month=YYYY-MM` replays the archived snapshot with
  **exact parity** to the live canonical payload — the dashboard renders
  historical months through the same renderer, growing pie charts and
  anomaly findings across all archived history.

## Officer Anomaly Detection (exact, objective)

For each month and metric (use-of-force events, incident involvement,
arrests linked, news-linked incidents):

- Peer group: same-agency officers with ≥1 recorded event of the metric in
  the window (self excluded, minimum 3 peers).
- Statistics: median, MAD, mean, max; robust z = (x − median)/(1.486×MAD)
  (reported as not calculable when MAD = 0); exact Poisson upper-tail p-value
  (`scipy.stats.poisson.sf`); Benjamini–Hochberg FDR correction across all
  officer-metric tests.
- Finding thresholds (disclosed in output): count ≥ 3 AND ≥ 2× peer median
  AND (q ≤ 0.05 OR robust z ≥ 3.5).
- Every officer’s exact counts remain fully visible in the officer metrics
  table regardless of findings — no omission of facts.

## Semantic Output

`app/analytics/narrative.py` renders every statistic into plain language:
deterministic, complete (all measured values, windows, peer statistics,
p/q-values, and record sources are stated), and strictly objective — a
subjective-marker audit (`audit_objectivity`) is enforced by tests.

## API

| Method | Path | Description |
|---|---|---|
| GET | `/` | Dashboard UI |
| GET | `/health` | Health + counts |
| GET | `/sources` | Catalog + live registry state (last run, errors, discovered datasets) |
| POST | `/pipeline/run-full` | Six-phase pipeline run |
| POST | `/ingest/run` | Run all enabled live sources |
| POST | `/synthesis/run` | Synthesize staged records |
| POST | `/resolve/pending` | Resolve suspended dependencies |
| POST | `/archive/refresh` | Trigger monthly archive (default: current month) |
| GET | `/pipeline/runs` | Full phase audit trail |
| GET | `/api/months` | Active + archived months |
| GET | `/api/analytics?month=` | Canonical analytics (live or archived replay) |
| GET | `/api/analytics/anomalies?month=` | Officer anomaly findings |
| GET | `/api/search?q=&mode=` | Hybrid retrieval (hybrid/lexical/semantic/literal) |
| GET | `/api/archive/files` | List immutable archive files |
| GET | `/api/archive/file/{id}` | Verified immutable download |
| GET | `/incidents` `/officers` `/links` `/staging/suspended` | Entity listings |

## Setup

```bash
cp .env.example .env      # optional: pin tokens (CourtListener / MuckRock)
poetry install
poetry run python -m app.api.scripts.init_db
poetry run uvicorn app.api.main:app --host 0.0.0.0 --port 8000
```

On startup the app runs one full six-phase pass (live fetch), then schedules
the 15-minute due-source runner and the monthly refresh. Railway deployment
uses the Dockerfile as-is; `DATABASE_URL` is auto-detected from Railway's
PostgreSQL add-on variables.

## Tests

```bash
poetry run pytest
```

66 tests cover: phase-order enforcement, checksum dedup, verification gating
and external revalidation, anomaly statistics (BH against reference values),
narrative objectivity audits, archive immutability/versioning/parity, hybrid
retrieval fusion, and catalog anti-fabrication invariants.
