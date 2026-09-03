# Police Lattice

Topological codebase for ingesting, mapping, and synthesizing heterogeneous
police accountability data from Tempe, Phoenix, Maricopa County, and
state-level sources.

## Overview

Police Lattice is an automated data acquisition, normalization, evidence
extraction, and synthesis pipeline that unifies heterogeneous public records,
open data portals, news feeds, court records, and manual document drops into
a single relational lattice. It is designed around explicit joining keys,
rule-based evidentiary extraction, and state suspension: records whose
cross-system dependencies are not yet satisfied are held in `suspended` status
until those dependencies arrive.

## Architecture

```
source_catalog.yaml          (defines 68 sources, access modes, schedules, join requirements)
        |
        v
Autonomous Acquisition       (ArcGIS, Socrata, FlatFile, CourtListener, MuckRock,
        |                     News RSS, PDF OCR, Audio, Public Records, OPD,
        |                     Web Scraper, Generic REST with fallback defaults)
        v
Raw Snapshots (SHA-256)      (Immutable raw JSON/text with deduplication)
        |
        v
Canonical Normalizer         (Standardizes datetime, agency names, badge numbers,
        |                     officer names, coordinates, and canonical entity types)
        v
Evidence Extraction Engine   (Precompiled regex NLP extractor identifying officers,
        |                     incidents, ARS statutes, force taxonomy, dockets)
        v
Staging Records              (pending / suspended / ready / synthesized / failed)
        |
        v
Dependency Resolver          (promotes suspended records once external keys arrive)
        |
        v
Synthesis Engine             (maps staging records & extracted evidence into unified
        |                     PostgreSQL lattice with topological entity links)
        v
PostgreSQL Lattice           (agencies, officers, incidents, arrests, charges,
                              complaints, court_cases, documents, news_articles,
                              surveillance_events, internal_affairs_cases,
                              monitor_reports, entity_links, synthesis_runs)
        |
        v
FastAPI Web UI & API         (Interactive dashboard, OpenAPI Swagger, and REST endpoints)
```

## Ingestion & Pipeline Layers

### 1. Autonomous Acquisition Layer
- **Resilient Polling**: Coordinates automated acquisition across all 68 catalog sources with thread-pool concurrency limits and strict request timeouts to conserve Railway compute.
- **Fallback Public Endpoints**: Built-in default public RSS and open data endpoints for verified public feeds.
- **Deduplication**: Computes SHA-256 checksums per raw record batch to skip duplicate ingestion across repeat runs.

### 2. Canonical Normalization Layer
- **Temporal Normalization**: Standardizes ISO 8601, Unix timestamps (seconds & milliseconds), RFC 2822, and standard US date formats into timezone-aware UTC `datetime`.
- **Agency Standardization**: Maps aliases (`"PHX PD"`, `"Phoenix Police"`, `"PPD"`, `"TPD"`, `"MCSO"`, `"AZ DPS"`, `"Mesa PD"`, etc.) to canonical Title Case names.
- **Identifier & Location Cleaning**: Strips badge formatting (`"#1042"`, `"Badge: B1042"` -> `"B1042"`), cleans street suffixes (`"Rd"`, `"Ave"`, `"St"`, `"Blvd"`), and normalizes intersection connectors (`"&"`).
- **Name Parsing**: Robust parsing of `"Last, First"`, `"Officer First Last"`, `"Det. David Kowalski"`, separating `first_name`, `last_name`, and canonical `rank`.

### 3. Evidence Extraction Layer
- **High-Efficiency Rule-Based NER**: Precompiled, zero-overhead regex matchers optimized for low-memory Railway environments (no heavy GPU/PyTorch dependencies).
- **Law Enforcement Personnel**: Extracts officer names, ranks (`Officer`, `Detective`, `Sergeant`, `Lieutenant`, `Captain`, `Chief`, `Deputy`, `Trooper`), badge numbers, and employee IDs from unstructured text.
- **Incident & CAD Extraction**: Identifies incident numbers, case files, CAD event numbers, and cross streets.
- **Force Taxonomy**: Classifies use of force events into standardized categories (`firearm_discharge`, `conducted_energy_weapon`, `physical_restraint`, `impact_weapon`, `chemical_agent`, `canine_deployment`, `vehicle_pursuit`).
- **Statutory & Legal Citations**: Detects Arizona Revised Statutes (`ARS 13-1204`, `ARS 13-2904`, `ARS 28-693`, `ARS 13-3407`, `ARS 13-1502`) with automated charge title enrichment and severity labeling (`Felony` / `Misdemeanor`), Brady list disclosures, Rule 15.1 disclosures, Section 1983 claims, and court docket numbers.

### 4. Topological Synthesis & Entity Linking
- Maps normalized payloads and extracted evidence into core entity tables (`Incident`, `Officer`, `Arrest`, `Charge`, `CourtCase`, `Document`, `NewsArticle`, `SurveillanceEvent`).
- Creates bidirectional `EntityLink` records connecting officers to incidents (`involved_in`), staging records to entities (`derived_from`), and news/court articles to incidents (`reports_on`, `evidence_for`).

## Database Schema

Key tables:
- **data_sources**: Registry of all configured sources with schedule and status.
- **raw_records**: Immutable snapshots of ingested data with SHA-256 checksums.
- **staging_records**: Intermediate normalized records with status tracking (`pending`, `suspended`, `ready`, `synthesized`, `failed`).
- **pending_synthesis**: Tracks unresolved cross-system dependencies.
- **agencies**: Law enforcement agencies with external ID mappings.
- **officers**: Individual officers linked to agencies via badge/employee IDs.
- **persons**: Subjects referenced across records.
- **incidents**: Events linked to agencies with temporal, spatial, and evidentiary data.
- **arrests**: Arrest records linked to incidents and persons.
- **charges**: Criminal charges linked to arrests (enriched with ARS statutes).
- **court_cases**: Court dockets with case numbers.
- **documents**: Ingested document text and metadata.
- **news_articles**: News feed entries with URLs, publication dates, and incident links.
- **surveillance_events**: Surveillance technology events (ALPR, etc.).
- **internal_affairs_cases**: Internal affairs investigations.
- **monitor_reports**: Federal monitor compliance reports.
- **entity_links**: Explicit relational links between lattice entities with confidence scores.
- **synthesis_runs**: Audit trail of synthesis execution history.

## API Endpoints

| Method | Path                 | Description                                      |
|--------|----------------------|--------------------------------------------------|
| GET    | `/`                  | Interactive Web UI Dashboard                     |
| GET    | `/health`            | Health check with staging record count            |
| POST   | `/pipeline/run-full` | Run end-to-end Acquisition -> Synthesis pipeline  |
| POST   | `/ingest/run`        | Trigger ingestion adapters                       |
| POST   | `/synthesis/run`     | Execute synthesis of staging records             |
| POST   | `/resolve/pending`   | Attempt to resolve suspended dependencies         |
| GET    | `/incidents`         | List incidents (paginated, newest first)          |
| GET    | `/officers`          | List officers (paginated)                         |
| GET    | `/links`             | List entity links (paginated)                     |
| GET    | `/staging/suspended` | List suspended staging records                    |

## Setup and Run

### Local Development

```bash
# Clone repository
git clone <repository-url> policelattice
cd policelattice

# Copy environment configuration
cp .env.example .env

# Install dependencies (Poetry)
poetry install

# Initialize database schema
poetry run python -m app.api.scripts.init_db

# Start FastAPI server (includes scheduler & automated startup pipeline)
poetry run uvicorn app.api.main:app --reload
```

### Docker & Railway Deployment

```bash
docker compose up -d
```
The Docker container automatically initializes the database, ingests seed drop data, synthesizes entities, starts background cron scheduling, and serves the web dashboard and API on port 8000.
