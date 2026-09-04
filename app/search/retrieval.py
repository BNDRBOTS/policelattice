"""Hybrid retrieval over the police-accountability lattice.

Best-in-class components per retrieval family:

- **Lexical**: ``bm25s`` — the fastest BM25 implementation for Python
  (Lucene-grade scoring, sparse matrices; published benchmarks show order-of-
  magnitude speedups over rank-bm25 with identical ranking semantics).
- **Semantic**: ``fastembed`` — ONNX-quantized ``BAAI/bge-small-en-v1.5``
  embeddings (top of the MTEB small-model retrieval category), CPU inference
  with no GPU/PyTorch dependency.
- **Literal**: ``rapidfuzz`` — the benchmark-standard C++ fuzzy string
  matching library (Levenshtein/Jaro-Winkler), used for exact- and
  near-exact literal matching of officer names, badge numbers, case numbers.

Fusion: Reciprocal Rank Fusion (RRF, k=60) — the standard, tunable-free
hybrid fusion used across modern retrieval systems.

The corpus is rebuilt from the live lattice whenever synthesis bumps the
index version (in-memory cache keyed by version).
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass, field
from typing import Any

import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings

logger = logging.getLogger(__name__)

TOKEN_RE = re.compile(r"[a-z0-9]+")
RRF_K = 60


@dataclass
class SearchHit:
    doc_id: str
    entity_type: str
    entity_id: int
    title: str
    snippet: str
    score: float
    components: dict[str, float] = field(default_factory=dict)


_index_version = 0
_index_lock = threading.Lock()


def bump_index_version() -> int:
    """Signal that the lattice changed and the retrieval corpus is stale."""
    global _index_version
    with _index_lock:
        _index_version += 1
        return _index_version


def current_index_version() -> int:
    return _index_version


# ---------------------------------------------------------------------------
# Corpus construction (live database only)
# ---------------------------------------------------------------------------

def build_corpus(session: Session) -> list[dict[str, Any]]:
    """Materialize the searchable corpus from live lattice rows."""
    from app.models import CourtCase, Document, Incident, NewsArticle, Officer

    docs: list[dict[str, Any]] = []

    for inc in session.scalars(select(Incident)).all():
        data = inc.data or {}
        text = " ".join(
            str(x)
            for x in [
                inc.incident_type,
                inc.location,
                data.get("force_type"),
                data.get("cause_of_death"),
                data.get("person_name"),
                data.get("victim_name"),
                data.get("agency_name"),
                (inc.external_ids or {}).get("incident_number"),
            ]
            if x
        )
        docs.append(
            {
                "doc_id": f"incident:{inc.id}",
                "entity_type": "incident",
                "entity_id": inc.id,
                "title": (
                    f"{(inc.external_ids or {}).get('incident_number') or inc.id}"
                    f" — {inc.incident_type or 'incident'}"
                ),
                "text": text,
            }
        )

    for off in session.scalars(select(Officer)).all():
        name = " ".join(filter(None, [off.first_name, off.last_name])) or "Name not recorded"
        text_parts = [
            name,
            off.badge_number,
            off.employee_id,
            (off.external_ids or {}).get("rank"),
            (off.external_ids or {}).get("agency_name"),
        ]
        text = " ".join(str(x) for x in text_parts if x)
        docs.append(
            {
                "doc_id": f"officer:{off.id}",
                "entity_type": "officer",
                "entity_id": off.id,
                "title": name,
                "text": text,
            }
        )

    for cc in session.scalars(select(CourtCase)).all():
        docs.append(
            {
                "doc_id": f"court_case:{cc.id}",
                "entity_type": "court_case",
                "entity_id": cc.id,
                "title": cc.case_number or f"case {cc.id}",
                "text": " ".join(str(x) for x in [cc.case_number, cc.court, cc.status] if x),
            }
        )

    for doc in session.scalars(select(Document)).all():
        text = ((doc.text or "") or "")[:20000]
        docs.append(
            {
                "doc_id": f"document:{doc.id}",
                "entity_type": "document",
                "entity_id": doc.id,
                "title": doc.title or f"document {doc.id}",
                "text": text,
            }
        )

    for art in session.scalars(select(NewsArticle)).all():
        docs.append(
            {
                "doc_id": f"news:{art.id}",
                "entity_type": "news_article",
                "entity_id": art.id,
                "title": art.title,
                "text": " ".join(str(x) for x in [art.title, art.content or ""] if x)[:20000],
            }
        )
    return docs


def _tokenize(text: str) -> list[str]:
    return TOKEN_RE.findall(text.lower())


# ---------------------------------------------------------------------------
# Hybrid retriever
# ---------------------------------------------------------------------------

class HybridRetriever:
    """bm25s + fastembed + rapidfuzz with Reciprocal Rank Fusion."""

    def __init__(self, session: Session):
        self.session = session
        self.settings = get_settings()
        self._docs: list[dict[str, Any]] | None = None
        self._bm25 = None
        self._bm25_corpus_tokens: list[list[str]] | None = None
        self._embeddings: np.ndarray | None = None
        self._embed_model = None
        self._built_version: int | None = None

    # -- lazy corpus --------------------------------------------------------

    def _ensure_corpus(self) -> list[dict[str, Any]]:
        global _index_version
        if self._docs is None or self._built_version != current_index_version():
            self._docs = build_corpus(self.session)
            self._build_lexical()
            self._build_semantic()
            self._built_version = current_index_version()
        return self._docs

    def _build_lexical(self) -> None:
        import bm25s

        texts = [f"{d['title']} {d['text']}" if d["text"] else d["title"] for d in self._docs]
        if not texts:
            self._bm25 = None
            return
        corpus_tokens = bm25s.tokenize(texts, stopwords=None, show_progress=False)
        self._bm25 = bm25s.BM25()
        self._bm25.index(corpus_tokens, show_progress=False)

    def _build_semantic(self) -> None:
        texts = [f"{d['title']} {d['text']}"[:4096] for d in self._docs]
        if not texts:
            self._embeddings = None
            return
        try:
            from fastembed import TextEmbedding

            if self._embed_model is None:
                self._embed_model = TextEmbedding(model_name=self.settings.semantic_model)
            vectors = list(self._embed_model.embed(texts, batch_size=32))
            self._embeddings = np.array([v for v in vectors], dtype=np.float32)
            # L2 normalize for cosine similarity via dot product
            norms = np.linalg.norm(self._embeddings, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            self._embeddings = self._embeddings / norms
        except Exception as exc:
            logger.warning("Semantic index unavailable (%s); lexical+literal remain active", exc)
            self._embeddings = None

    # -- component searches ---------------------------------------------------

    def _lexical_search(self, query: str, top_k: int) -> list[tuple[int, float]]:
        if self._bm25 is None:
            return []
        import bm25s

        q_tokens = bm25s.tokenize(
            [query], stopwords=None, show_progress=False, return_ids=False
        )[0]
        if not q_tokens:
            return []
        scores = np.asarray(self._bm25.get_scores(q_tokens), dtype=np.float64).ravel()
        top = np.argsort(-scores)[:top_k]
        return [(int(i), float(scores[i])) for i in top if scores[i] > 0]

    def _semantic_search(self, query: str, top_k: int) -> list[tuple[int, float]]:
        if self._embeddings is None or self._embed_model is None:
            return []
        try:
            qvec = np.array(next(self._embed_model.embed([query])), dtype=np.float32)
            norm = np.linalg.norm(qvec)
            if norm > 0:
                qvec = qvec / norm
            sims = self._embeddings @ qvec
            top = np.argsort(-sims)[:top_k]
            return [(int(i), float(sims[i])) for i in top if sims[i] > 0.05]
        except Exception as exc:
            logger.warning("Semantic query failed: %s", exc)
            return []

    def _literal_search(self, query: str, top_k: int) -> list[tuple[int, float]]:
        """Exact + fuzzy literal matching (names, badges, case numbers)."""
        from rapidfuzz import fuzz

        q = query.strip()
        if not q:
            return []
        q_lower = q.lower()
        scored: list[tuple[int, float]] = []
        for idx, doc in enumerate(self._docs):
            text = (doc["text"] or "").lower()
            title = doc["title"].lower()
            s = 0.0
            if q_lower in title or q_lower in text:
                s = 100.0
            else:
                tokens = _tokenize(f"{title} {text}")
                best_token = max(
                    (fuzz.ratio(q_lower, token) for token in tokens), default=0.0
                )
                # partial ratio catches multi-word names typed with typos
                best_phrase = max(
                    fuzz.partial_ratio(q_lower, title),
                    fuzz.partial_ratio(q_lower, text[:400]),
                )
                best = max(best_token, best_phrase)
                if best >= 85:
                    s = best
            if s > 0:
                scored.append((idx, s))
        scored.sort(key=lambda x: -x[1])
        return scored[:top_k]

    # -- fused search -----------------------------------------------------------

    def search(
        self, query: str, limit: int | None = None, mode: str = "hybrid"
    ) -> dict[str, Any]:
        limit = limit or self.settings.search_result_limit
        docs = self._ensure_corpus()

        components: dict[str, list[tuple[int, float]]] = {}
        if mode in ("hybrid", "lexical"):
            components["lexical"] = self._lexical_search(query, 100)
        if mode in ("hybrid", "semantic"):
            components["semantic"] = self._semantic_search(query, 100)
        if mode in ("hybrid", "literal"):
            components["literal"] = self._literal_search(query, 100)

        # Reciprocal Rank Fusion
        fused: dict[int, float] = {}
        comp_ranks: dict[str, dict[int, int]] = {}
        for name, results in components.items():
            for rank, (idx, _score) in enumerate(results):
                fused[idx] = fused.get(idx, 0.0) + 1.0 / (RRF_K + rank + 1)
                comp_ranks.setdefault(name, {})[idx] = rank + 1

        ordered = sorted(fused.items(), key=lambda x: -x[1])[:limit]
        hits: list[dict[str, Any]] = []
        for idx, rrf_score in ordered:
            doc = docs[idx]
            hit_components = {}
            for name in components:
                rank = comp_ranks.get(name, {}).get(idx)
                if rank is not None:
                    raw = dict(components[name]).get(idx)
                    hit_components[name] = {"rank": rank, "raw_score": raw}
            snippet = (doc["text"] or "")[:300]
            hits.append(
                {
                    "doc_id": doc["doc_id"],
                    "entity_type": doc["entity_type"],
                    "entity_id": doc["entity_id"],
                    "title": doc["title"],
                    "snippet": snippet,
                    "rrf_score": round(rrf_score, 6),
                    "components": hit_components,
                }
            )

        return {
            "query": query,
            "mode": mode,
            "index_version": current_index_version(),
            "corpus_size": len(docs),
            "semantic_backend": (
                self.settings.semantic_model if self._embeddings is not None else "unavailable"
            ),
            "results": hits,
        }
