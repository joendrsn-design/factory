"""
test_magpie.py — regression tests for the Magpie Diagnostics substrate content line.

Runnable without pytest: `python test_magpie.py` (exit 1 on any failure).
All tests are offline — no LLM, no network (Research uses NoSearchProvider; QA is fed
a canned response; Deposit runs dry-run with a stubbed artifact loader).

Covers the five required guarantees:
  1. Substrate enumeration -> 110 candidates with stable ids (regression lock).
  2. _provenance never reaches Write (topic -> research -> planning -> write prompt).
  3. A Magpie PUBLISH verdict becomes PUBLISH_PENDING_REVIEW and Deposit holds it.
  4. A biomarker topic with unconfirmable therapy linkage is blocked, not written.
  5. Non-Magpie pipeline behavior is unchanged.
"""
import json

from magpie_common import is_magpie, carry_magpie
from topic_generator import TopicGenerator
from research import ResearchModule, NoSearchProvider
from planning import PlanningModule
from write import WriteModule

CANCERS = ["breast", "colon", "lung", "prostate", "lymphoma", "ovarian"]
CONFIG = "config/sites"


def _p3_breast_topic():
    tg = TopicGenerator(config_dir=CONFIG)
    topics = tg.generate_for_site("magpie-diagnostics", count=50, run_id="test")
    return next(mb for mb in topics
                if mb[0]["article_type"] == "P3_biomarker_overview"
                and mb[0]["magpie"]["cancer"] == "breast")


def test_substrate_enumeration_110():
    from magpie_substrate import enumerate_all
    t1 = enumerate_all("substrate", CANCERS)
    assert len(t1) == 110, f"expected 110 spokes, got {len(t1)}"
    by_type = {}
    for t in t1:
        by_type[t.article_type] = by_type.get(t.article_type, 0) + 1
    assert by_type == {"A_classification": 54, "B_biomarker": 56}, by_type
    # stable ids: a second enumeration yields identical topic_ids
    t2 = enumerate_all("substrate", CANCERS)
    assert [t.topic_id for t in t1] == [t.topic_id for t in t2], "topic_ids not stable"
    assert len({t.topic_id for t in t1}) == 110, "topic_ids not unique"


def test_pillar_enumeration():
    from magpie_pillars import enumerate_pillars, enumerate_thematic
    pillars = enumerate_pillars("substrate", CANCERS)
    by_type = {}
    for p in pillars:
        by_type[p.article_type] = by_type.get(p.article_type, 0) + 1
    # P1/P2/P3/P4 all active (one per cancer = 24).
    assert by_type == {
        "P1_explainer": 6,
        "P2_classification_overview": 6,
        "P3_biomarker_overview": 6,
        "P4_recent_developments": 6,
    }, by_type
    assert len({p.topic_id for p in pillars}) == 24, "pillar topic_ids not unique"
    # P1 consumes the epidemiology dataset, with _provenance stripped.
    p1 = next(p for p in pillars if p.pillar_code == "P1")
    assert "epidemiology" in p1.consumes and p1.consumes["epidemiology"], "P1 missing epi data"
    assert "_provenance" not in json.dumps(p1.consumes), "_provenance in P1 consumes"
    assert p1.references, "P1 has no resolved references"
    # P4 needs external research (no substrate references; verify replaced by assembly).
    p4 = next(p for p in pillars if p.pillar_code == "P4")
    assert p4.needs_external and not p4.needs_verify, "P4 external flags wrong"
    # Thematic Pulse line: D (Liquid, ctDNA-derived) + C (Digital, curated themes file).
    th = enumerate_thematic("substrate", CANCERS)
    by = {}
    for t in th:
        by[t.article_type] = by.get(t.article_type, 0) + 1
    assert by.get("D_liquid", 0) >= 1, by
    assert by.get("C_digital", 0) == 6, f"expected 6 curated Digital Pulse themes, got {by}"
    assert all(t.needs_external and t.cancer == "" for t in th), "thematic flags wrong"
    assert all(t.section in ("liquid-pulse", "digital-pulse") for t in th), "thematic routing wrong"
    assert len({t.topic_id for t in th}) == len(th), "thematic topic_ids not unique"
    assert "_provenance" not in json.dumps([t.consumes for t in th]), "_provenance in thematic consumes"


def test_ab_spoke_chain():
    tg = TopicGenerator(config_dir=CONFIG)
    sc = tg.loader.load("magpie-diagnostics")
    topics = tg.generate_for_site("magpie-diagnostics", count=999, run_id="t")
    for at, key in [("A_classification", "entity"), ("B_biomarker", "biomarker")]:
        meta, body = next(mb for mb in topics
                          if mb[0]["article_type"] == at and mb[0]["magpie"].get("cancer") == "breast")
        assert "_provenance" not in json.dumps(meta), f"{at} topic has _provenance"
        assert meta["magpie"]["section"] == "breast", f"{at} section routing wrong"
        r = ResearchModule(config_dir=CONFIG)
        r.search_provider = NoSearchProvider()
        r._magpie_verify = lambda blob, sc: (True, [], [], [])  # not testing the verify gate here
        rmeta, rbody = r.run_single(meta, body, "")
        assert rmeta.get("status") == "complete", f"{at} research not complete"
        pm, pb = PlanningModule(config_dir=CONFIG).run_single(rmeta, rbody, "")
        assert pm["slug"] and len(pm["slug"].split("-")) <= 5, f"{at} slug too long: {pm['slug']}"
        system, user = WriteModule(config_dir=CONFIG).build_prompt(pm, pb, sc)
        rec = meta["magpie"]["source_record"]
        assert rec.get(key, "")[:15] in user, f"{at} record not injected into Write"
        assert "breast cancer" in user.lower(), f"{at} cancer not generalized (Type A was hardcoded)"
        assert "_provenance" not in (system + user), f"{at} _provenance in Write prompt"


def test_external_research_path():
    tg = TopicGenerator(config_dir=CONFIG)
    sc = tg.loader.load("magpie-diagnostics")
    topics = tg.generate_for_site("magpie-diagnostics", count=999, run_id="t")
    p4 = next(mb for mb in topics
              if mb[0]["article_type"] == "P4_recent_developments" and mb[0]["magpie"]["cancer"] == "breast")

    # No search provider -> blocked (external types REQUIRE sources; never write unsourced).
    r = ResearchModule(config_dir=CONFIG)
    r.search_provider = NoSearchProvider()
    rm, _ = r.run_single(p4[0], p4[1], "")
    assert rm.get("status") == "blocked", f"expected blocked offline, got {rm.get('status')}"

    # Confirmed developments reach Write; unconfirmed surface in the report, not in Write.
    fake_conf = [{"headline": "FDA approved companion dx for HER2-ultralow", "date": "01/2025",
                  "status": "approved", "what_it_changes": "x", "connects_to": ["HER2"],
                  "citation": {"title": "T", "source": "NEJM", "year": "2025", "id": "10.x"},
                  "confidence": "confirmed"}]
    r._magpie_external_assemble = lambda blob, scx: (
        fake_conf, [{"headline": "UNCONFIRMED_ITEM", "confidence": "unconfirmed"}],
        [{"source_id": "10.x", "title": "T", "url": "10.x", "type": "external", "year": "2025"}], "")
    rm2, rb2 = r.run_single(p4[0], p4[1], "")
    assert rm2.get("status") == "complete", "confirmed developments should pass"
    assert any("UNCONFIRMED_ITEM" in json.dumps(e) for e in rm2.get("excluded_unconfirmed", [])), \
        "excluded_unconfirmed not surfaced for the report"
    pm, pb = PlanningModule(config_dir=CONFIG).run_single(rm2, rb2, "")
    system, user = WriteModule(config_dir=CONFIG).build_prompt(pm, pb, sc)
    assert "HER2-ultralow" in user, "confirmed development not injected into Write"
    assert "UNCONFIRMED_ITEM" not in user, "unconfirmed development leaked into Write"

    # Zero confirmed -> blocked (can't write a freshness piece with no sources).
    r._magpie_external_assemble = lambda blob, scx: ([], [{"headline": "x", "confidence": "unconfirmed"}], [], "")
    rm3, _ = r.run_single(p4[0], p4[1], "")
    assert rm3.get("status") == "blocked", "zero-confirmed should block"


def test_no_provenance_reaches_write():
    # The raw biomarker substrate DOES contain _provenance (so the test is meaningful).
    raw = json.load(open("substrate/breast/breast_biomarkers.json", encoding="utf-8"))
    assert any("_provenance" in r for r in raw["biomarker_records"]), "fixture lost _provenance"

    meta, body = _p3_breast_topic()
    assert "_provenance" not in json.dumps(meta), "_provenance in topic metadata"

    r = ResearchModule(config_dir=CONFIG)
    r.search_provider = NoSearchProvider()
    r._magpie_verify = lambda blob, sc: (True, [], [], [])  # not testing the verify gate here
    rmeta, rbody = r.run_single(meta, body, "")
    assert "_provenance" not in json.dumps(rmeta), "_provenance in research metadata"
    assert "_provenance" not in rbody, "_provenance in research brief"

    plan_meta, plan_body = PlanningModule(config_dir=CONFIG).run_single(rmeta, rbody, "")
    assert "_provenance" not in json.dumps(plan_meta), "_provenance in plan metadata"

    w = WriteModule(config_dir=CONFIG)
    sc = w.loader.load("magpie-diagnostics")
    system, user = w.build_prompt(plan_meta, plan_body, sc)
    assert "_provenance" not in system and "_provenance" not in user, "_provenance in WRITE prompt"


def test_magpie_publish_becomes_pending_and_deposit_holds():
    import qa as qa_mod
    from qa import QAModule
    qm = QAModule(config_dir=CONFIG)
    sc = qm.loader.load("magpie-diagnostics")

    meta, _ = _p3_breast_topic()
    input_meta = {
        "run_id": "t", "article_id": "a1", "site_id": "magpie-diagnostics",
        "article_type": "P3_biomarker_overview", "title": "Breast Biomarkers",
        "slug": "breast-biomarker-overview", "category": "breast",
        "word_count": 1200, "magpie": meta["magpie"],
    }
    clean_body = "# Breast Biomarkers\n\nThe evidence suggests molecular testing guides therapy class [1].\n\n## References\n1. ref\n"
    qa_json = json.dumps({
        "verdict": "PUBLISH", "composite_score": 9.0,
        "scores": {"factual_accuracy": 9}, "feedback": "good", "rewrite_instructions": "",
    })
    out_meta, out_body = qm.parse_response(qa_json, input_meta, clean_body, sc)
    assert out_meta["verdict"] == "PUBLISH_PENDING_REVIEW", out_meta["verdict"]
    assert is_magpie(out_meta), "magpie blob not carried through QA"
    # validate_output (run by run_single after parse) must accept the gated verdict.
    ok, err = qm.validate_output(out_meta, out_body)
    assert ok, f"validate_output rejected PUBLISH_PENDING_REVIEW: {err}"

    # Deposit must hold it (dry-run), not publish.
    from deposit import DepositEngine
    arts = [(out_meta, clean_body, "x.md")]
    orig = qa_mod  # placeholder to avoid lints
    import deposit as dep
    saved = dep.load_artifacts_from_dir
    dep.load_artifacts_from_dir = lambda *a, **k: arts
    try:
        summary = DepositEngine(config_dir=CONFIG).deposit(dry_run=True)
    finally:
        dep.load_artifacts_from_dir = saved
    assert len(summary["pending_review"]) == 1, summary
    assert len(summary["published"]) == 0, "Magpie article was published instead of held"


def test_blocked_topic_not_written():
    meta, body = _p3_breast_topic()
    r = ResearchModule(config_dir=CONFIG)
    r.search_provider = NoSearchProvider()
    # Stub the verifier to fail a therapy-linkage claim (unconfirmable).
    r._magpie_verify = lambda blob, sc: (False, ["BR-BIO-0001 (unconfirmable)"], [], ["no current source"])

    rmeta, rbody = r.run_single(meta, body, "")
    assert rmeta.get("status") == "blocked", f"expected blocked, got {rmeta.get('status')}"
    assert "BR-BIO-0001" in rmeta.get("block_reason", ""), rmeta.get("block_reason")

    # A blocked artifact is excluded by the status_filter='complete' the next stage uses.
    import tempfile, os
    from artifacts import save_artifact, load_artifacts_from_dir
    d = tempfile.mkdtemp()
    save_artifact(rmeta, rbody, d)
    picked = load_artifacts_from_dir(d, status_filter="complete")
    assert picked == [], "blocked artifact would be picked up by the next stage"

    # And planning refuses it directly (defense in depth).
    try:
        PlanningModule(config_dir=CONFIG).run_single(rmeta, rbody, "")
        raise AssertionError("planning did not refuse a blocked Magpie artifact")
    except ValueError as e:
        assert "non-complete" in str(e), str(e)


def test_blocked_topic_stays_retry_eligible():
    # A blocked artifact must NOT count as "done" in dedup, or a transient block
    # (network/flaky assembly) silently drops the topic from every future run.
    import tempfile
    from artifacts import save_artifact, research_metadata
    from topic_generator import PublishingHistory
    d = tempfile.mkdtemp()
    done = research_metadata(run_id="r", article_id="a1", site_id="magpie-diagnostics",
                             article_type="P4_recent_developments", topic="t", research_depth="deep",
                             source_count=1, key_findings=["x"], sources=[{"source_id": "s"}],
                             from_cache=False, statistics=[])
    done["topic_id"] = "tid-complete"
    blocked = research_metadata(run_id="r", article_id="a2", site_id="magpie-diagnostics",
                                article_type="P4_recent_developments", topic="t2", research_depth="deep",
                                source_count=0, key_findings=[], sources=[], from_cache=False, statistics=[])
    blocked["topic_id"] = "tid-blocked"
    blocked["status"] = "blocked"
    save_artifact(done, "body one " * 40, d)
    save_artifact(blocked, "body two " * 40, d)

    hist = PublishingHistory(pipeline_dirs=[d])
    ids = hist.get_existing_topic_ids("magpie-diagnostics")
    assert "tid-complete" in ids, "completed topic should be deduped"
    assert "tid-blocked" not in ids, "blocked topic must stay retry-eligible (not deduped)"


def test_firewall_gates_on_is_magpie_not_config_flag():
    # The PUBLISH->PUBLISH_PENDING_REVIEW conversion must key on is_magpie(metadata),
    # not the separate magpie.review_required config flag (which can be mis-set).
    from qa import QAModule
    qm = QAModule(config_dir=CONFIG)
    sc = qm.loader.load("magpie-diagnostics")
    meta, _ = _p3_breast_topic()
    base = {"run_id": "t", "article_id": "a", "site_id": "magpie-diagnostics",
            "article_type": "P3_biomarker_overview", "title": "T", "slug": "s", "word_count": 1100}
    body = "# T\n\nThe evidence suggests testing guides therapy class [1].\n\n## References\n1. ref\n"
    qa_json = json.dumps({"verdict": "PUBLISH", "composite_score": 9.0, "scores": {}, "feedback": "", "rewrite_instructions": ""})

    mag = dict(base); mag["magpie"] = meta["magpie"]
    out_mag, _ = qm.parse_response(qa_json, mag, body, sc)
    assert out_mag["verdict"] == "PUBLISH_PENDING_REVIEW", "Magpie PUBLISH must be gated regardless of review_required"

    # A non-Magpie article on the same site must NOT be converted, and must NOT have
    # category force-threaded (that would re-taxonomize non-Magpie content).
    non = dict(base)
    out_non, _ = qm.parse_response(qa_json, non, body, sc)
    assert out_non["verdict"] == "PUBLISH", "non-Magpie verdict must stay PUBLISH"
    assert "category" not in out_non or out_non.get("category") == "", "non-Magpie category must not be force-threaded"


def test_ref_needs_verify_reads_structured_key():
    # The WHO5 reference carries a structured `verify` directive whose text contains no
    # literal 'verify' — the flag must still be honored (read the key, not the values).
    from magpie_substrate import _ref_needs_verify
    idx = {"who5_breast": {"title": "Breast Tumours", "verify": "Confirm subtype list and any corrigenda."},
           "plain": {"title": "Some paper", "year": "2020"}}
    assert _ref_needs_verify(["who5_breast"], idx) is True, "structured verify key ignored"
    assert _ref_needs_verify(["plain"], idx) is False
    assert _ref_needs_verify(["missing"], idx) is False


def test_verify_fails_closed_without_search_provider():
    # No search provider => a therapy-linkage (B) topic must BLOCK, not silently pass.
    meta, body = next(mb for mb in TopicGenerator(config_dir=CONFIG)
                      .generate_for_site("magpie-diagnostics", count=999, run_id="t")
                      if mb[0]["article_type"] == "B_biomarker" and mb[0]["magpie"].get("cancer") == "breast")
    r = ResearchModule(config_dir=CONFIG)
    r.search_provider = NoSearchProvider()
    rmeta, _ = r.run_single(meta, body, "")
    assert rmeta.get("status") == "blocked", "B therapy-linkage must fail closed with no search provider"
    assert "no search provider" in rmeta.get("block_reason", "").lower()


def test_overview_flags_not_blocks():
    # A P3 biomarker OVERVIEW (pillar) must FLAG unconfirmable therapy-linkage and still
    # write (cited synthesis + human review) — unlike the spoke, which blocks.
    meta, body = _p3_breast_topic()
    r = ResearchModule(config_dir=CONFIG)
    r.search_provider = NoSearchProvider()
    rmeta, rbody = r.run_single(meta, body, "")
    assert rmeta.get("status") == "complete", "P3 overview must flag-not-block, not block"
    assert "FLAGGED" in rbody or any("FLAGGED" in str(n) for n in rmeta.get("magpie", {}).get("notes", [])) \
        or "flag" in rbody.lower(), "overview should surface a flag note for unverified linkage"


def test_external_sources_come_from_developments():
    # P4/C/D meta[sources] must be built from confirmed external developments (their
    # references dict is empty), so QA isn't handed an empty source list.
    from write import WriteModule
    w = WriteModule(config_dir=CONFIG)
    sc = w.loader.load("magpie-diagnostics")
    plan_meta = {"run_id": "t", "article_id": "a", "site_id": "magpie-diagnostics",
                 "article_type": "P4_recent_developments", "title": "T", "slug": "s",
                 "magpie": {"topic_id": "x", "article_type": "P4_recent_developments", "references": {},
                            "external_developments": [
                                {"headline": "h", "citation": {"title": "Paper A", "year": "2025", "id": "10.x"}}]}}
    art = "# T\n\nBody [1].\n\n## References\n1. Paper A\n"
    meta, _ = w.parse_response(art, plan_meta, "", sc)
    assert len(meta["sources"]) == 1 and meta["sources"][0]["title"] == "Paper A", meta["sources"]


def test_non_magpie_unchanged():
    # is_magpie / carry_magpie are inert for non-Magpie artifacts.
    assert is_magpie({}) is False
    assert is_magpie({"topic": "x"}) is False
    out = {"a": 1}
    carry_magpie({"topic": "x"}, out)
    assert out == {"a": 1}, "carry_magpie mutated a non-Magpie artifact"

    # Research.validate_output for a non-Magpie artifact still uses the original thresholds.
    r = ResearchModule(config_dir=CONFIG)
    ok, err = r.validate_output({"research_depth": "moderate", "key_findings": [], "sources": []}, "x" * 50)
    assert ok is False and "too short" in err, (ok, err)

    # Write.validate_output for a non-Magpie artifact still uses the 85-130% band.
    w = WriteModule(config_dir=CONFIG)
    ok, err = w.validate_output({"word_count": 100, "target_word_count": 1000, "site_id": "lamphill"}, "# t\nbody")
    assert ok is False and "under target" in err, (ok, err)


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failures = 0
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
        except Exception as e:
            failures += 1
            print(f"FAIL  {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    return failures


if __name__ == "__main__":
    import sys
    sys.exit(1 if _run_all() else 0)
