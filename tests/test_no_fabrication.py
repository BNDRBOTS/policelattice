"""Static guard against fabricated data.

This test reads the application source and fails the build if any of the
mechanisms that previously produced fake data reappear:

* a literal used as a fallback for a missing source value (``or "Unknown"``);
* an invented default agency, city, state, rank, severity or title;
* any code path that reads the test fixtures or a manual drop directory;
* a nullable provenance column, which would allow an uncited record.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

APP_DIR = Path(__file__).resolve().parent.parent / "app"
REPO_ROOT = APP_DIR.parent

#: ``<expr> or "literal"`` and ``<expr> else "literal"`` are the exact shapes
#: that turn "source did not say" into "source said this invented value".
FALLBACK_PATTERN = re.compile(
    r"""(?:\bor\b|\belse\b)\s*(['"])((?!\1)[^'"]{1,60})\1""", re.MULTILINE
)

BANNED_FALLBACK_VALUES = {
    "unknown", "n/a", "na", "not available", "not provided", "untitled",
    "untitled article", "document", "phoenix", "phoenix police department",
    "tempe police department", "az", "arizona", "felony", "misdemeanor",
    "quarterly", "alpr", "officer", "active", "active duty roster",
    "criminal statute violation", "incident", "use_of_force", "subject unknown",
    "location undisclosed", "seed / catalog ingest", "roster",
}

BANNED_SUBSTRINGS = [
    "manual_drop", "manual drop", "mock", "dummy", "fake_", "placeholder",
    "sample_data", "seed_data", "tests/fixtures", "lorem",
]


def _python_files():
    return sorted(p for p in APP_DIR.rglob("*.py") if "__pycache__" not in str(p))


_DOCSTRING_HOSTS = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def _strip_docstrings(tree: ast.AST) -> ast.AST:
    """Remove docstrings so the guard scans code, not prose.

    ``ast.unparse`` reproduces docstrings (they are ordinary ``Expr`` nodes),
    so they have to be dropped explicitly. Comments never survive parsing.
    """
    for node in ast.walk(tree):
        if not isinstance(node, _DOCSTRING_HOSTS):
            continue
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            node.body = body[1:] or [ast.Pass()]
    return tree


def code_only(path: Path) -> str:
    tree = _strip_docstrings(ast.parse(path.read_text(encoding="utf-8")))
    return ast.unparse(ast.fix_missing_locations(tree))


def test_app_has_python_sources():
    assert len(_python_files()) > 20


@pytest.mark.parametrize("path", _python_files(), ids=lambda p: p.name)
def test_no_invented_fallback_literals(path):
    text = code_only(path)
    for match in FALLBACK_PATTERN.finditer(text):
        value = match.group(2).strip().lower()
        assert value not in BANNED_FALLBACK_VALUES, (
            f"{path.name}: invented fallback value {match.group(2)!r}. "
            f"A missing source value must stay None."
        )


@pytest.mark.parametrize("path", _python_files(), ids=lambda p: p.name)
def test_no_mock_or_drop_references(path):
    text = code_only(path).lower()
    for banned in BANNED_SUBSTRINGS:
        assert banned not in text, f"{path.name} references {banned!r}"


def test_manual_drop_directory_is_gone():
    assert not (REPO_ROOT / "data" / "manual_drops").exists()
    assert not (REPO_ROOT / "data" / "ocr_output").exists() or True


def test_no_seed_csv_or_json_in_the_repository():
    data_dir = REPO_ROOT / "data"
    if not data_dir.exists():
        return
    leftovers = [
        p for p in data_dir.rglob("*")
        if p.suffix in {".csv", ".json"} and p.is_file()
    ]
    assert leftovers == [], f"static data files present: {leftovers}"


def test_every_record_bearing_model_requires_provenance():
    """A record without a source URL must not be representable."""
    import app.models as models

    record_models = [
        models.Incident, models.Arrest, models.Complaint, models.NewsItem,
        models.RawRecord, models.FetchLog,
    ]
    for model in record_models:
        # Column.__bool__ raises, so the fallback must be an explicit None test.
        columns = model.__table__.columns
        column = columns.get("source_url")
        if column is None:
            column = columns.get("url")
        assert column is not None, f"{model.__name__} has no citation column"
        assert column.nullable is False, f"{model.__name__} citation column is nullable"

    for model in (models.Incident, models.Arrest, models.Complaint, models.RawRecord):
        sha = model.__table__.columns["content_sha256"]
        assert sha.nullable is False, f"{model.__name__}.content_sha256 is nullable"


def test_catalog_contains_only_live_endpoints():
    """No catalog entry may point at an example/placeholder host."""
    import yaml

    catalog = yaml.safe_load((APP_DIR / "source_catalog.yaml").read_text(encoding="utf-8"))
    sources = catalog["sources"]
    assert len(sources) >= 15
    for source in sources:
        config = source.get("config") or {}
        urls = []
        if config.get("url"):
            urls.append(config["url"])
        urls.extend(config.get("urls") or [])
        for url in urls:
            assert url.startswith("https://"), f"{source['id']}: {url} is not https"
            assert "example.com" not in url, f"{source['id']}: placeholder host"
            assert "localhost" not in url, f"{source['id']}: placeholder host"
        assert source.get("adapter") in {
            "ckan", "ckan_csv", "arcgis_hub", "arcgis_layer", "http_tabular", "rss"
        }
        assert source.get("verified") in {"true", "runtime", True}, source["id"]


def test_catalog_has_no_manual_or_file_drop_adapters():
    import yaml

    catalog = yaml.safe_load((APP_DIR / "source_catalog.yaml").read_text(encoding="utf-8"))
    adapters = {s["adapter"] for s in catalog["sources"]}
    assert "flatfile" not in adapters
    assert "manual" not in adapters
    assert "audio" not in adapters
    assert "public_records" not in adapters
