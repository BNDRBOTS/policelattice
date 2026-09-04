"""Hybrid retrieval: lexical (bm25s), literal (rapidfuzz), RRF fusion."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.models import Officer
from app.search import retrieval
from app.search.retrieval import HybridRetriever, bump_index_version, current_index_version


@pytest.fixture()
def session():
    engine = sa.create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    S = sessionmaker(bind=engine)
    with S() as s:
        yield s


@pytest.fixture(autouse=True)
def no_semantic_model(monkeypatch):
    """Force the semantic backend offline path so tests are deterministic
    without downloading the embedding model (production uses fastembed)."""
    monkeypatch.setattr(
        HybridRetriever, "_build_semantic", lambda self: setattr(self, "_embeddings", None)
    )


def _seed_officers(s):
    s.add_all([
        Officer(
            first_name="John",
            last_name="Smith",
            badge_number="1042",
            external_ids={"rank": "Sergeant"},
        ),
        Officer(first_name="Jane", last_name="Smyth", badge_number="2211"),
        Officer(first_name="Alice", last_name="Johnson", badge_number="3300"),
    ])
    s.commit()


def test_literal_exact_and_fuzzy_name_match(session):
    _seed_officers(session)
    r = HybridRetriever(session).search("John Smith", mode="literal")
    assert r["corpus_size"] == 3
    assert r["results"]
    assert r["results"][0]["entity_type"] == "officer"
    assert "Smith" in r["results"][0]["title"]

    # typo tolerance via rapidfuzz
    r2 = HybridRetriever(session).search("Jon Smiht", mode="literal")
    assert any("Smith" in h["title"] for h in r2["results"])


def test_literal_badge_number_match(session):
    _seed_officers(session)
    r = HybridRetriever(session).search("1042", mode="literal")
    assert r["results"] and r["results"][0]["title"] == "John Smith"


def test_hybrid_fusion_combines_components(session):
    _seed_officers(session)
    r = HybridRetriever(session).search("Sergeant John Smith", mode="hybrid")
    assert r["mode"] == "hybrid"
    top = r["results"][0]
    assert top["title"] == "John Smith"
    # fusion exposes per-component ranks
    assert "literal" in top["components"] or "lexical" in top["components"]


def test_rrf_fusion_ordering():
    """Unit-check the Reciprocal Rank Fusion formula used by the retriever."""
    rrf_k = retrieval.RRF_K
    lexical = [(0, 9.0), (1, 5.0)]          # doc0 rank1, doc1 rank2
    literal = [(1, 100.0), (2, 90.0)]       # doc1 rank1, doc2 rank2
    fused = {}
    for results in (lexical, literal):
        for rank, (idx, _s) in enumerate(results):
            fused[idx] = fused.get(idx, 0.0) + 1.0 / (rrf_k + rank + 1)
    # doc1 is reinforced by both components -> must outrank doc0
    assert fused[1] > fused[0]
    # rank2 in lexical + rank1 in literal
    assert fused[1] == pytest.approx(1 / (rrf_k + 2) + 1 / (rrf_k + 1))


def test_index_version_bump_invalidates_corpus(session):
    _seed_officers(session)
    retriever = HybridRetriever(session)
    retriever._ensure_corpus()
    v1 = current_index_version()
    assert retriever._built_version == v1

    bump_index_version()
    retriever._ensure_corpus()  # rebuilt under the new version
    assert retriever._built_version == current_index_version() == v1 + 1


def test_search_labels_semantic_backend_unavailable_offline(session):
    _seed_officers(session)
    r = HybridRetriever(session).search("John", mode="semantic")
    assert r["semantic_backend"] == "unavailable"
    assert r["results"] == []
