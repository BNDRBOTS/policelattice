Police Lattice

Topological codebase for ingesting, mapping, and synthesizing heterogeneous
police accountability data from Tempe, Phoenix, Maricopa County, and state-level
sources.

## Architecture

- **Source Catalog** (`app/source_catalog.yaml`) defines every source, its
  physical access mode, schedule, and join requirements.
- **Ingestion Adapters** implement the exact physical constraints of each
  source: ArcGIS FeatureServer pagination, Socrata SoQL, flat-file drops,
  OCR, public-records portals, CourtListener, MuckRock, RSS, and manual
  audio/document imports.
- **Pipeline Runner** executes adapters during their availability windows,
  stores raw snapshots, and creates staging records.
- **State Suspension** holds staging records in `suspended` status when
  external cross-system keys are not yet available.
- **Resolver** periodically promotes suspended records once dependencies
  arrive.
- **Synthesis** maps staging records into the unified PostgreSQL lattice using
  only explicit joining keys.
- **API** exposes query and reconciliation endpoints.

## Run

```bash
cp .env.example .env
docker compose up -d db
poetry install
poetry run python scripts/init_db.py
poetry run uvicorn app.api.main:app --reload
```

Ingestion scheduler runs inside the API process.
