"""Read-only health snapshot of the live DB. SELECT/count only, no writes."""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from p4_gates import get_supabase_client


def count(db, table, **filters):
    """Exact count with optional eq filters; None value means IS NULL / NOT NULL handled by caller."""
    q = db.table(table).select("*", count="exact", head=True)
    for k, v in filters.items():
        q = q.eq(k, v)
    return q.execute().count


def safe(label, fn):
    try:
        return fn()
    except Exception as e:
        print(f"  ! {label}: ERROR {e}")
        return None


def main():
    db = get_supabase_client()
    now = datetime.now(timezone.utc)
    d7 = (now - timedelta(days=7)).isoformat()
    d30 = (now - timedelta(days=30)).isoformat()

    print("=" * 60)
    print("SITES")
    print("=" * 60)
    safe("sites total", lambda: print(f"  total sites: {count(db, 'sites')}"))
    safe("sites active", lambda: print(f"  active: {count(db, 'sites', active=True)}"))
    safe("auto_publish", lambda: print(f"  auto_publish_enabled: {count(db, 'sites', auto_publish_enabled=True)}"))

    print("\n" + "=" * 60)
    print("CONTENT by status")
    print("=" * 60)
    def content_statuses():
        rows = db.table("content").select("status").execute().data or []
        agg = {}
        for r in rows:
            agg[r.get("status")] = agg.get(r.get("status"), 0) + 1
        print(f"  total content rows: {len(rows)}")
        for s, c in sorted(agg.items(), key=lambda x: -x[1]):
            print(f"    {str(s):<20} {c}")
    safe("content statuses", content_statuses)

    print("\n  --- QA / persona coverage on content ---")
    def coverage():
        total = count(db, "content")
        with_persona = db.table("content").select("*", count="exact", head=True).not_.is_("persona_id", "null").execute().count
        qa_passed = db.table("content").select("*", count="exact", head=True).not_.is_("qa_passed_at", "null").execute().count
        print(f"  content w/ persona_id: {with_persona}/{total}")
        print(f"  content w/ qa_passed_at: {qa_passed}/{total}")
    safe("coverage", coverage)

    def gate_results_col():
        n = db.table("content").select("*", count="exact", head=True).not_.is_("qa_gate_results", "null").execute().count
        print(f"  content w/ qa_gate_results populated: {n}")
    safe("qa_gate_results (new col)", gate_results_col)

    print("\n  --- recency ---")
    safe("created 7d", lambda: print(f"  content created last 7d:  {db.table('content').select('*', count='exact', head=True).gte('created_at', d7).execute().count}"))
    safe("created 30d", lambda: print(f"  content created last 30d: {db.table('content').select('*', count='exact', head=True).gte('created_at', d30).execute().count}"))
    safe("published 30d", lambda: print(f"  published last 30d:       {db.table('content').select('*', count='exact', head=True).eq('status','published').gte('published_at', d30).execute().count}"))

    print("\n" + "=" * 60)
    print("PERSONAS / site_personas coverage")
    print("=" * 60)
    safe("personas", lambda: print(f"  personas total: {count(db,'personas')}  active: {count(db,'personas',active=True)}  tier1-active: {count(db,'personas',active=True,tier=1)}"))
    def coverage_gap():
        sites = {r["domain"] for r in (db.table("sites").select("domain").execute().data or [])}
        linked = {r["site_domain"] for r in (db.table("site_personas").select("site_domain").execute().data or [])}
        missing = sorted(sites - linked)
        print(f"  sites with >=1 persona linked: {len(sites & linked)}/{len(sites)}")
        if missing:
            print(f"  sites MISSING persona link ({len(missing)}): {', '.join(missing)}")
    safe("persona coverage gap", coverage_gap)

    print("\n" + "=" * 60)
    print("QA FAILURES")
    print("=" * 60)
    def qa_failures():
        rows = db.table("qa_failures").select("status").execute().data or []
        agg = {}
        for r in rows:
            agg[r.get("status")] = agg.get(r.get("status"), 0) + 1
        print(f"  total qa_failures: {len(rows)}")
        for s, c in sorted(agg.items(), key=lambda x: -x[1]):
            print(f"    {str(s):<20} {c}")
    safe("qa_failures", qa_failures)

    print("\n" + "=" * 60)
    print("EMBEDDINGS (OpenAI quota signal)")
    print("=" * 60)
    for tbl in ("content_embeddings", "embeddings"):
        safe(tbl, lambda t=tbl: print(f"  {t}: {count(db, t)} rows"))


if __name__ == "__main__":
    main()
