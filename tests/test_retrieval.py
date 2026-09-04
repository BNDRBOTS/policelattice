"""Hybrid retrieval: BM25 lexical leg, RRF fusion, honest degradation."""

from __future__ import annotations

from app.pipeline.retrieval import RRF_K, HybridRetriever, _rrf


def test_rrf_fusion_prefers_documents_both_methods_rank():
    lexical = {0: 3.0, 1: 2.0, 2: 1.0}
    semantic = {1: 0.9, 2: 0.8, 3: 0.7}
    fused = dict(_rrf(lexical, semantic, RRF_K))
    assert max(fused, key=fused.get) == 1     # ranked by both
    assert fused[0] == 1.0 / (RRF_K + 1)      # lexical rank 1 only


def test_rrf_handles_a_single_list():
    fused = dict(_rrf({5: 1.0}, {}, RRF_K))
    assert list(fused) == [5]


def test_index_and_lexical_search(memory_session):
    from datetime import UTC, datetime

    from app.models import Agency, Incident

    memory_session.add(Agency(id="phoenix-pd", name="Phoenix Police Department"))
    memory_session.flush()
    for index, (force, weapon) in enumerate([
        ("Level 2 Use of Force", "Taser"),
        ("Level 3 Use of Force", "Firearm"),
        ("Level 1 Use of Force", None),
    ]):
        memory_session.add(
            Incident(
                agency_id="phoenix-pd", external_number=f"INC-{index}", kind="use_of_force",
                occurred_at=datetime(2025, 6, 10 + index, tzinfo=UTC), period="2025-06",
                highest_force_applied=force, armed_type=weapon,
                source_id="test", source_url=f"https://example.test/rec/{index}",
                retrieved_at=datetime.now(UTC), content_sha256=f"{index:064d}", data={},
            )
        )
    memory_session.commit()

    retriever = HybridRetriever()
    status = retriever.build(memory_session, "2025-06")
    assert status["documents"] == 3
    assert status["lexical"] is True
    assert status["fusion"] == f"reciprocal_rank_fusion(k={RRF_K})"

    result = retriever.search("taser", k=3)
    assert result["result_count"] > 0
    assert result["results"][0]["source_url"] == "https://example.test/rec/0"
    assert result["results"][0]["lexical_score"] is not None

    # every hit carries the URL of the record it came from
    for hit in result["results"]:
        assert hit["source_url"].startswith("https://")


def test_semantic_unavailability_is_reported_not_hidden(memory_session, monkeypatch):
    """With SEMANTIC_SEARCH off the response says so explicitly."""
    from app.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "semantic_search", False)

    retriever = HybridRetriever()
    status = retriever.build(memory_session, None)
    assert status["semantic"] is False
    assert status["semantic_error"] == "disabled by SEMANTIC_SEARCH=false"
    assert status["embedding_model"] is None
