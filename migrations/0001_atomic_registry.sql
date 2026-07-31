-- Migration 0001: atomic registry updates
-- =========================================
-- Fixes audit finding H4 (registry.py): concurrent runs for the same site
-- lost counter increments via read-modify-write, and register_site could
-- create duplicate rows when no unique constraint existed.
--
-- Apply once against the Supabase/Postgres database, e.g.:
--   psql "$SUPABASE_DB_URL" -f migrations/0001_atomic_registry.sql
--
-- After applying, registry.record_run() automatically uses the atomic RPC;
-- until then it transparently falls back to read-modify-write.

-- 1) Guarantee site_key uniqueness so register_site's upsert/409 handling and
--    the RPC's lookup are well-defined. (No-op if the constraint already exists.)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'factory_registry_site_key_key'
    ) THEN
        ALTER TABLE factory_registry
            ADD CONSTRAINT factory_registry_site_key_key UNIQUE (site_key);
    END IF;
END$$;

-- 2) Atomic increment of run totals + schedule/failure bookkeeping.
--    A single UPDATE with `col = col + delta` is atomic under Postgres row
--    locking, so overlapping runs can no longer clobber each other's counts.
CREATE OR REPLACE FUNCTION increment_site_totals(
    p_site_key            text,
    p_last_run_at         timestamptz,
    p_next_run_at         timestamptz,
    p_status              text,
    p_error               text,
    p_articles_generated  integer,
    p_articles_published  integer,
    p_articles_killed     integer,
    p_rewrites            integer,
    p_cost_cents          integer
) RETURNS void
LANGUAGE sql
AS $$
    UPDATE factory_registry SET
        last_run_at              = p_last_run_at,
        next_run_at              = p_next_run_at,
        total_runs               = total_runs + 1,
        total_articles_generated = total_articles_generated + p_articles_generated,
        total_articles_published = total_articles_published + p_articles_published,
        total_articles_killed    = total_articles_killed + p_articles_killed,
        total_rewrites           = total_rewrites + p_rewrites,
        total_cost_cents         = total_cost_cents + p_cost_cents,
        consecutive_failures     = CASE WHEN p_status = 'success'
                                        THEN 0
                                        ELSE consecutive_failures + 1 END,
        last_error               = CASE WHEN p_status = 'success'
                                        THEN NULL
                                        ELSE p_error END
    WHERE site_key = p_site_key;
$$;
