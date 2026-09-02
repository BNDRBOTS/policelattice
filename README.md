# Police Lattice

Topological codebase for ingesting, mapping, and synthesizing heterogeneous
police accountability data from Tempe, Phoenix, Maricopa County, and
state-level sources.

## Overview

Police Lattice is a data ingestion and synthesis pipeline that unifies
heterogeneous public records, open data portals, news feeds, court records,
and manual document drops into a single relational lattice. It is designed
around explicit joining keys and state suspension: records whose cross-system
dependencies are not yet satisfied are held in `suspended` status until those
dependencies arrive.

## Architecture

```
source_catalog.yaml          (defines every source, its access mode, schedule, and join requirements)
        |
        v
Ingestion Adapters           (arcgis, socrata, flatfile, courtlistener, muckrock,
        |                     news_rss, pdf_ocr, audio, public_records, opd,
        |                     web_scraper, generic_rest)
        v
Pipeline Runner              (executes adapters during availability windows,
        |                     stores raw snapshots, creates staging records)
        v
Staging Records              (pending / suspended / ready / failed)
        |
        v
Dependency Resolver          (promotes suspended records once external keys arrive)
        |
        v
Synthesis Engine             (maps staging records into unified PostgreSQL lattice
        |                     using only explicit joining keys)
        v
PostgreSQL Lattice           (agencies, officers, incidents, arrests, charges,
                              complaints, court_cases, documents, news_articles,
                              surveillance_events, internal_affairs_cases,
                              monitor_reports, entity_links, synthesis_runs)
        |
        v
FastAPI REST API             (query and reconciliation endpoints)
```

## Source Catalog

The file `app/source_catalog.yaml` declares 68 sources across the following
categories:

| Category                  | Description                                             |
|---------------------------|---------------------------------------------------------|
| official_data_portals     | Tempe PD, Phoenix PD, OpenPoliceData, AZ DPS            |
| specialized_dashboards    | Phoenix UOF, OIS, PGP, SOF, RAIDS dashboards           |
| surveillance_technology   | Flock Safety ALPR transparency portals                  |
| oversight                 | Phoenix OAT, Civilian Review Board, AZPOST               |
| legal_court               | CourtListener, AZ Judicial Branch, MCAO, Brady List     |
| investigative             | Police Scorecard, MuckRock, Mapping Police Violence     |
| public_records            | GovQA, Records Sections, Professional Standards         |
| live_audio                | Maricopa County police scanner feeds                    |
| news                      | ABC15, AZCentral, FOX10, KTAR, ProPublica, others       |
| other                     | Mesa Transparency, MCSO Inmate Search, predictive tools |

## Ingestion Adapters

Each adapter implements the physical constraints of its source type:

| Adapter           | Source Types                                      | Access Mode |
|-------------------|---------------------------------------------------|-------------|
| `arcgis`          | ArcGIS FeatureServer with pagination              | api         |
| `socrata`         | Socrata open data portals via SoQL                | api         |
| `flatfile`        | CSV, Excel, JSON, NDJSON file drops               | file_drop   |
| `courtlistener`   | CourtListener REST API v4 dockets                 | api         |
| `muckrock`        | MuckRock FOIA request API                         | api         |
| `news_rss`        | RSS/Atom news feeds                               | rss         |
| `pdf_ocr`         | PDF documents with text extraction and OCR        | manual      |
| `audio`           | Audio file metadata from manual drops             | manual      |
| `public_records`  | Portal exports placed in drop directory           | manual      |
| `opd`             | OpenPoliceData Python library                     | api         |
| `web_scraper`     | Static HTML public portals                        | manual      |
| `generic_rest`    | Generic JSON REST endpoints                       | api         |

## Database Schema

All tables use PostgreSQL with JSONB columns for flexible metadata storage.
Key tables:

- **data_sources**: Registry of all configured sources with schedule and status.
- **raw_records**: Immutable snapshots of ingested data with SHA-256 checksums.
- **staging_records**: Intermediate records with status tracking (pending/suspended/ready/failed).
- **pending_synthesis**: Tracks unresolved cross-system dependencies.
- **agencies**: Law enforcement agencies with external ID mappings.
- **officers**: Individual officers linked to agencies via badge/employee IDs.
- **persons**: Subjects referenced across records.
- **incidents**: Events linked to agencies with temporal and spatial data.
- **complaints**: Formal complaints filed against agencies.
- **arrests**: Arrest records linked to incidents and persons.
- **charges**: Criminal charges linked to arrests.
- **court_cases**: Court dockets with case numbers.
- **documents**: Ingested document text and metadata.
- **news_articles**: News feed entries with URLs and publication dates.
- **surveillance_events**: Surveillance technology events (ALPR, etc.).
- **internal_affairs_cases**: Internal affairs investigations.
- **monitor_reports**: Federal monitor compliance reports.
- **entity_links**: Explicit relational links between lattice entities.
- **synthesis_runs**: Audit trail of synthesis execution history.

## State Suspension and Resolution

When a staging record cannot be synthesized because a required external key
does not yet exist in the lattice (e.g., a use-of-force record references an
officer badge number not yet ingested), the record is suspended and a
`PendingSynthesis` entry is created. The `DependencyResolver` periodically
checks pending entries against the current lattice. When the required key
arrives, the staging record is promoted to `ready` status for synthesis.

After 10 unsuccessful resolution attempts, a pending dependency is marked
`expired`.

## API Endpoints

| Method | Path                | Description                                |
|--------|---------------------|--------------------------------------------|
| GET    | `/health`           | Health check with staging record count      |
| POST   | `/ingest/run`       | Trigger all due ingestion adapters          |
| POST   | `/synthesis/run`    | Execute synthesis of pending staging records|
| POST   | `/resolve/pending`  | Attempt to resolve suspended dependencies   |
| GET    | `/incidents`        | List incidents (paginated, newest first)    |
| GET    | `/officers`         | List officers (paginated)                   |
| GET    | `/links`            | List entity links (paginated)               |
| GET    | `/staging/suspended`| List suspended staging records              |

## Configuration

All configuration is loaded from environment variables or a `.env` file.
See `.env.example` for the full list of configurable variables:

- **Core**: `DATABASE_URL`, `APP_ENV`, `LOG_LEVEL`
- **Tempe PD**: ArcGIS FeatureServer URLs for calls, offenses, arrests, hate crimes, sentiment
- **Phoenix PD**: ArcGIS URLs, Socrata domain, dataset IDs for UOF/OIS/PGP/SOF
- **External APIs**: `COURTLISTENER_TOKEN`, `MUCKROCK_TOKEN`, `MUCKROCK_USERNAME`
- **News Feeds**: RSS URLs for ABC15, AZCentral, FOX10, KTAR, ProPublica, and others
- **Manual/OCR**: `MANUAL_DROP_DIR`, `PDF_OCR_OUTPUT_DIR`, `TESSERACT_CMD`

## Setup and Run

### Prerequisites

- Python 3.11+
- PostgreSQL 16+
- Tesseract OCR (for PDF OCR adapter)
- poppler-utils (for PDF text extraction)

### Local Development

```bash
# Clone and enter the project
git clone <repository-url> policelattice
cd policelattice

# Copy environment configuration and edit as needed
cp .env.example .env

# Start the database
docker compose up -d db

# Install Python dependencies
poetry install

# Initialize the database schema
poetry run python -m app.api.scripts.init_db

# Start the API server (includes ingestion scheduler)
poetry run uvicorn app.api.main:app --reload
```

The ingestion scheduler runs inside the API process, checking for due sources
every 15 minutes.

### Docker Deployment

```bash
docker compose up -d
```

The Docker container runs database initialization on startup and then serves
the FastAPI application on port 8000.

### Railway Deployment

The project includes `railway.toml` configured to use the Dockerfile for
building and deploying. Set the `DATABASE_URL` and any source-specific
environment variables in the Railway dashboard.

## Manual Data Drops

For sources with `access_mode: manual`, place files in the configured
`MANUAL_DROP_DIR` (default: `./data/manual_drops`):

- **PDF documents**: Placed as `.pdf` files; text is extracted automatically,
  with OCR fallback via Tesseract.
- **CSV/Excel/JSON**: Placed as `.csv`, `.xlsx`, `.xls`, `.json`, or `.ndjson`
  files and parsed by the flatfile adapter.
- **Audio files**: Placed as `.mp3` files; metadata is captured.
- **Portal exports**: Any supported file format placed in the drop directory
  is processed by the public_records adapter.

## Project Structure

```
policelattice/
  app/
    __init__.py
    config.py                     Pydantic settings from environment/.env
    db.py                         SQLAlchemy engine and session factory
    models.py                     All ORM models (PostgreSQL JSONB)
    source_catalog.yaml           Source definitions (68 sources)
    api/
      __init__.py
      main.py                     FastAPI application with REST endpoints
      scripts/
        init_db.py                Database schema initialization script
    ingestion/
      __init__.py
      base.py                     BaseAdapter, RawRecordDTO, AdapterRegistry
      arcgis.py                   ArcGIS FeatureServer adapter
      socrata.py                  Socrata SoQL adapter
      flatfile.py                 CSV/Excel/JSON/NDJSON file adapter
      courtlistener.py            CourtListener API adapter
      muckrock.py                 MuckRock FOIA API adapter
      news_rss.py                 RSS/Atom feed adapter
      pdf_ocr.py                  PDF text extraction and OCR adapter
      audio.py                    Audio file metadata adapter
      public_records.py           Public records portal export adapter
      opd.py                      OpenPoliceData library adapter
      web_scraper.py              Static HTML web scraper adapter
      generic_rest.py             Generic JSON REST endpoint adapter
    pipeline/
      __init__.py
      runner.py                   Pipeline execution and adapter registration
      scheduler.py                APScheduler cron-based scheduling
      state.py                    Staging record state transitions
      resolver.py                 Dependency resolution for suspended records
      synthesis.py                Staging-to-lattice synthesis engine
  docker-compose.yml              PostgreSQL + API service definitions
  Dockerfile                      Production container build
  pyproject.toml                  Python dependencies (Poetry)
  railway.toml                    Railway deployment configuration
  .env.example                    Environment variable template
```

## Design Principles

1. **Explicit joining only**: No relational connection is created unless the
   raw data provides an explicit joining key.
2. **No fabricated access**: Manual sources return empty records; the system
   never guesses URLs or credentials.
3. **State suspension**: Records with unsatisfied dependencies are held, not
   discarded or forced.
4. **Immutable raw layer**: Raw records are never modified after ingestion;
   synthesis operates on staging copies.
5. **Audit trail**: Every synthesis run is recorded with timestamps and stats.
6. **Temporal precision**: All timestamps use timezone-aware UTC datetimes.
7. **Retry with backoff**: External API calls use exponential backoff via
   tenacity.
