"""Hybrid retrieval: lexical + semantic, fused by Reciprocal Rank Fusion.

* **Lexical** — ``bm25s`` (Okapi BM25 over a sparse CSC index). It is the
  fastest published Python BM25 implementation and, unlike a hand-rolled
  scorer, handles the sparse term/document matrix without materializing it.
* **Semantic** — ``fastembed`` running ``BAAI/bge-small-en-v1.5`` through
  ONNX Runtime on CPU. Dense vectors catch the paraphrases BM25 misses
  ("tased" vs "conducted energy weapon").
* **Fusion** — Reciprocal Rank Fusion with ``k=60``, the standard constant
  from the original RRF paper. RRF is used rather than score blending because
  BM25 and cosine scores are not on a common scale.

The index is content-addressed: it is rebuilt only when the underlying corpus
hash changes, so repeated queries do not re-embed. If the embedding model
cannot be loaded, retrieval degrades to lexical-only and says so explicitly
in the response — it never silently presents lexical results as hybrid.
"""

from __future__ import annotations

import hashlib
import logging
import re
import threading
from dataclasses import dataclass, field
from typing import Any

import bm25s
import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.ingest.parsers import dumps
from app.models import (
    Arrest,
    Complaint,
    Incident,
    NewsItem,
    OfficerFinding,
    OfficerRef,
)

logger = logging.getLogger(__name__)
settings = get_settings()

RRF_K = 60
_WHITESPACE = re.compile(r"\s+")


@dataclass
class Document:
    doc_id: str
    kind: str
    text: str
    source_url: str | None
    period: str | None
    meta: dict[str, Any] = field(default_factory=dict)


class HybridRetriever:
    """Builds and queries a hybrid index over the current lattice."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._documents: list[Document] = []
        self._bm25: bm25s.BM25 | None = None
        self._vectors: np.ndarray | None = None
        self._corpus_hash: str | None = None
        self._embedder: Any = None
        self._embedder_error: str | None = None

    # -- corpus ------------------------------------------------------------
    def _corpus(self, session: Session, period: str | None) -> list[Document]:
        documents: list[Document] = []

        query = select(Incident)
        if period:
            query = query.where(Incident.period == period)
        for incident in session.execute(query).scalars().all():
            documents.append(
                Document(
                    doc_id=f"incident:{incident.id}",
                    kind="incident",
                    text=_join(
                        incident.kind,
                        incident.external_number,
                        incident.force_level,
                        incident.highest_force_applied,
                        incident.armed_type,
                        incident.resistance,
                        incident.de_escalation,
                        incident.injury,
                        incident.highest_charge,
                        incident.outcome,
                        incident.location,
                        incident.precinct,
                        incident.subject_race_group,
                        incident.subject_age_group,
                    ),
                    source_url=incident.source_url,
                    period=incident.period,
                    meta={
                        "external_number": incident.external_number,
                        "occurred_at": (
                            incident.occurred_at.isoformat() if incident.occurred_at else None
                        ),
                        "agency_id": incident.agency_id,
                        "content_sha256": incident.content_sha256,
                    },
                )
            )

        query = select(Arrest)
        if period:
            query = query.where(Arrest.period == period)
        for arrest in session.execute(query).scalars().all():
            documents.append(
                Document(
                    doc_id=f"arrest:{arrest.id}",
                    kind="arrest",
                    text=_join(
                        arrest.charge, arrest.charge_code, arrest.disposition,
                        arrest.location, arrest.precinct,
                    ),
                    source_url=arrest.source_url,
                    period=arrest.period,
                    meta={"external_number": arrest.external_number},
                )
            )

        query = select(Complaint)
        if period:
            query = query.where(Complaint.period == period)
        for complaint in session.execute(query).scalars().all():
            documents.append(
                Document(
                    doc_id=f"complaint:{complaint.id}",
                    kind="complaint",
                    text=_join(
                        complaint.category, complaint.allegation, complaint.finding,
                        complaint.discipline, complaint.status,
                    ),
                    source_url=complaint.source_url,
                    period=complaint.period,
                    meta={"external_number": complaint.external_number},
                )
            )

        query = select(OfficerFinding)
        if period:
            query = query.where(OfficerFinding.period == period)
        for finding in session.execute(query).scalars().all():
            documents.append(
                Document(
                    doc_id=f"finding:{finding.id}",
                    kind="finding",
                    text=_join(
                        finding.finding_type, finding.metric, finding.narrative,
                        finding.severity,
                    ),
                    source_url=(finding.sources or [None])[0],
                    period=finding.period,
                    meta={
                        "officer_ref_id": finding.officer_ref_id,
                        "p_value": finding.p_value,
                        "severity": finding.severity,
                        "sources": finding.sources,
                    },
                )
            )

        query = select(NewsItem)
        if period:
            query = query.where(NewsItem.period == period)
        for item in session.execute(query).scalars().all():
            documents.append(
                Document(
                    doc_id=f"news:{item.id}",
                    kind="news",
                    text=_join(item.title, item.summary),
                    source_url=item.url,
                    period=item.period,
                    meta={"source_id": item.source_id},
                )
            )

        for officer in session.execute(select(OfficerRef)).scalars().all():
            documents.append(
                Document(
                    doc_id=f"officer:{officer.id}",
                    kind="officer",
                    text=_join(
                        officer.external_key, officer.rank, officer.gender,
                        officer.race_group,
                    ),
                    source_url=officer.source_url,
                    period=None,
                    meta={"agency_id": officer.agency_id},
                )
            )
        return documents

    # -- embedding ---------------------------------------------------------
    def _get_embedder(self) -> Any:
        if self._embedder is not None or self._embedder_error is not None:
            return self._embedder
        if not settings.semantic_search:
            self._embedder_error = "disabled by SEMANTIC_SEARCH=false"
            return None
        try:
            from fastembed import TextEmbedding

            self._embedder = TextEmbedding(
                model_name=settings.embedding_model,
                cache_dir=settings.embedding_cache_dir,
            )
        except Exception as exc:  # noqa: BLE001
            self._embedder_error = f"{type(exc).__name__}: {exc}"
            logger.warning("Semantic embeddings unavailable: %s", self._embedder_error)
        return self._embedder

    def _embed(self, texts: list[str]) -> np.ndarray | None:
        embedder = self._get_embedder()
        if embedder is None or not texts:
            return None
        try:
            vectors = [np.asarray(v, dtype=np.float32) for v in embedder.embed(texts)]
            matrix = np.vstack(vectors)
            norms = np.linalg.norm(matrix, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            return matrix / norms
        except Exception as exc:  # noqa: BLE001
            self._embedder_error = f"{type(exc).__name__}: {exc}"
            return None

    # -- indexing ----------------------------------------------------------
    def build(self, session: Session, period: str | None = None) -> dict[str, Any]:
        """Build (or reuse) the index for ``period``."""
        documents = self._corpus(session, period)
        digest_input = dumps([d.doc_id for d in documents]) + (period or "all").encode()
        corpus_hash = hashlib.sha256(digest_input).hexdigest()

        with self._lock:
            if self._corpus_hash == corpus_hash and self._bm25 is not None:
                return self.status()
            self._documents = documents
            self._corpus_hash = corpus_hash
            if documents:
                tokens = bm25s.tokenize(
                    [d.text for d in documents], show_progress=False, allow_empty=True
                )
                self._bm25 = bm25s.BM25()
                self._bm25.index(tokens)
            else:
                self._bm25 = None
            self._vectors = self._embed([d.text for d in documents])
        return self.status()

    def status(self) -> dict[str, Any]:
        return {
            "documents": len(self._documents),
            "lexical": self._bm25 is not None,
            "semantic": self._vectors is not None,
            "semantic_error": self._embedder_error,
            "corpus_sha256": self._corpus_hash,
            "fusion": f"reciprocal_rank_fusion(k={RRF_K})",
            "embedding_model": settings.embedding_model if self._vectors is not None else None,
        }

    # -- querying ----------------------------------------------------------
    def search(self, query: str, *, k: int = 25, period: str | None = None) -> dict[str, Any]:
        if not self._documents or self._bm25 is None:
            return {"query": query, "results": [], **self.status()}

        k = max(1, min(int(k), len(self._documents)))
        pool = min(max(k * 4, k), len(self._documents))

        query_tokens = bm25s.tokenize([query], show_progress=False, allow_empty=True)
        lexical_hits = self._bm25.retrieve(query_tokens, k=pool, return_as="tuple")
        lexical_rank = {
            int(doc_idx): float(score)
            for doc_idx, score in zip(lexical_hits.documents[0], lexical_hits.scores[0])
        }

        semantic_rank: dict[int, float] = {}
        if self._vectors is not None:
            query_vector = self._embed([query])
            if query_vector is not None:
                similarities = (self._vectors @ query_vector[0]).astype(float)
                order = np.argsort(-similarities)[:pool]
                for index in order:
                    semantic_rank[int(index)] = float(similarities[index])

        fused = _rrf(lexical_rank, semantic_rank, RRF_K)
        results = []
        for position, (index, score) in enumerate(fused[:k], start=1):
            document = self._documents[index]
            results.append(
                {
                    "rank": position,
                    "score": round(score, 6),
                    "id": document.doc_id,
                    "kind": document.kind,
                    "text": _WHITESPACE.sub(" ", document.text).strip(),
                    "period": document.period,
                    "source_url": document.source_url,
                    "lexical_score": lexical_rank.get(index),
                    "semantic_score": semantic_rank.get(index),
                    "meta": document.meta,
                }
            )
        return {
            "query": query,
            "period": period,
            "result_count": len(results),
            "results": results,
            **self.status(),
        }


def _rrf(
    lexical: dict[int, float], semantic: dict[int, float], k: int
) -> list[tuple[int, float]]:
    """Reciprocal Rank Fusion over two ranked lists."""
    scores: dict[int, float] = {}
    for ranked in (lexical, semantic):
        for position, index in enumerate(sorted(ranked, key=lambda i: -ranked[i]), start=1):
            scores[index] = scores.get(index, 0.0) + 1.0 / (k + position)
    return sorted(scores.items(), key=lambda item: -item[1])


def _join(*values: Any) -> str:
    return " ".join(str(v) for v in values if v is not None and str(v).strip())


#: Process-wide retriever.
_retriever = HybridRetriever()


def get_retriever() -> HybridRetriever:
    return _retriever
