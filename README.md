# Police Lattice

Autonomous police-accountability data pipeline. Every figure it shows comes
from a live public source, fetched at run time, with the originating record
address attached. There is no sample data, no seeded dataset, and no
placeholder anywhere in the code path.

## What it does

Each run executes six phases, in this order, and will not reorder or skip them:

| Phase | What happens |
|---|---|
| **Search** | Discover and verify every configured source. `package_list`, DCAT feeds and resource manifests are resolved live so a source that adds or removes a resource is picked up without a code change. |
| **Gather** | Paginated fetch of every discovered resource, with server-side date sorting and a bounded look-back window. |
| **Organize** | Content-addressed storage of each raw record, with its fetch provenance. |
| **Process** | Parse to canonical entity types, then resolve officer entities by the identifier the source itself publishes as the officer key — never by name similarity. |
| **Verify** | Per-record citation checks, provenance completeness, cross-source consistency and coverage gaps. A `fail` verdict blocks the run and prevents a month from being sealed. |
| **Synthesize** | Persist entities with full provenance, run per-officer statistical anomaly detection, generate findings, and index for hybrid retrieval. |

After that, the analytics engine seals the month into an immutable,
content-addressed snapshot.

## Anomaly detection

Per-officer statistical isolation, computed programmatically from the data:

- **Peer group** — officers in the same agency and month. An officer is only
  evaluated against at least 30 peers and needs at least 5 events of their own.
- **Robust z-score** — median/MAD rather than mean/stddev, so one extreme
  officer cannot inflate the spread and hide everyone else.
- **Event volume** — exact Poisson upper tail, not a normal approximation.
- **Out-of-policy rate** — exact one-sided binomial in log space.
- **Severity** — `high` at p < 0.01 and z ≥ 3; `elevated` at p < 0.05 and z ≥ 2.

Every finding persists its numerator, denominator, peer median, peer MAD,
z-score, p-value, plain-language narrative and clickable source links.

## Monthly archive

`build_view()` produces one payload. Its SHA-256 (excluding the volatile
`generated_at` timestamp) is the archive key:

- same hash → `unchanged`, the row is never rewritten
- changed content → `revision + 1`, the previous row flips `is_current`
- **no row is ever mutated or deleted**

`/api/month/{period}` serves the sealed payload when one exists, so a
historical month renders with exactly the same structure as the live one —
same top-level keys, same chart shapes, same table columns. Switching months
is a single payload swap against a fixed DOM, so nothing shifts on redraw.

## Running it

```bash
poetry install
uvicorn app.api.main:app --host 0.0.0.0 --port 8000
```

On Railway a Postgres plugin supplies `DATABASE_URL`; without one the app
falls back to SQLite under `./data` so a local run works with no setup.

### Verify the sources yourself

```bash
python -m scripts.verify_sources                      # probe all of them
python -m scripts.verify_sources --source phoenix_ckan_uof --rows 3
```

This makes real requests and prints the HTTP status, the row count each source
advertises, and sample rows so you can see the actual column names. Nothing is
cached or stubbed.

### Tests

```bash
poetry run pytest        # 118 tests
poetry run ruff check app tests scripts
```

The CKAN transport test runs a real HTTP server on localhost and serves
recorded Phoenix payloads through the actual `httpx`/adapter/SQLAlchemy path.
The synthetic officer fixture is test-only, is never reachable from `app/`,
and `tests/test_no_fabrication.py` statically enforces that.

## Sources

17 sources across four adapters: CKAN (Phoenix), ArcGIS Hub (Tempe), HTTP
tabular (national CSVs), RSS (local news). `app/source_catalog.yaml` records,
for each entry, whether the endpoint was verified by hand, and the date,
status code, row count and field names observed at that time. Anything not
hand-verified is marked `verified: runtime` and is probed on every run, with
the true result written to `fetch_logs` and shown in the UI.

Two coverage gaps are published rather than papered over:

- Phoenix **Adult Arrests** covers Jan 2018 – Dec 2025; updates are
  unavailable from Jan 2026 while the city moves from SRS to NIBRS.
- Phoenix **Use of Force** has a documented ~3-month publication lag.

## What this environment cannot do

The sandbox this was developed in has outbound access to `pypi.org` only.
Live CKAN, ArcGIS, national CSV and RSS endpoints are all unreachable from
here, and the embedding model cannot be downloaded. End-to-end acquisition,
anomaly detection on real officer data, and the UI rendering were therefore
**not** exercised in the sandbox. Run `scripts/verify_sources.py` on a machine
with internet access to confirm the live path before trusting any figure.
