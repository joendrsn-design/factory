-- ============================================================
-- BATCH JOBS TABLE
-- ============================================================
-- Tracks Anthropic Batch API jobs for autonomous pipeline.
-- Each row represents one stage submission (research, planning, write, qa).
--
-- Run this in Supabase SQL Editor.
-- ============================================================

-- Create batch_jobs table
CREATE TABLE IF NOT EXISTS batch_jobs (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,

    -- Job identification
    run_id TEXT NOT NULL,                    -- Factory run ID (e.g., run_20260412_060000_abc123)
    site_key TEXT NOT NULL,                  -- Site this batch is for
    stage TEXT NOT NULL,                     -- Pipeline stage: research, planning, write, qa
    batch_id TEXT NOT NULL,                  -- Anthropic Batch API ID (e.g., msgbatch_xxx)

    -- Status tracking
    status TEXT NOT NULL DEFAULT 'pending',  -- pending, processing, completed, failed

    -- Metrics
    article_count INTEGER NOT NULL DEFAULT 0, -- Number of articles in this batch
    cost_cents INTEGER DEFAULT 0,             -- Estimated cost in cents

    -- Timestamps
    submitted_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    -- Error tracking
    error_message TEXT,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_batch_jobs_status ON batch_jobs(status);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_site_key ON batch_jobs(site_key);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_run_id ON batch_jobs(run_id);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_batch_id ON batch_jobs(batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_stage_status ON batch_jobs(stage, status);

-- Unique constraint: one batch per stage per run per site
CREATE UNIQUE INDEX IF NOT EXISTS idx_batch_jobs_unique
ON batch_jobs(run_id, site_key, stage);

-- Update timestamp trigger
CREATE OR REPLACE FUNCTION update_batch_jobs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS batch_jobs_updated_at ON batch_jobs;
CREATE TRIGGER batch_jobs_updated_at
    BEFORE UPDATE ON batch_jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_batch_jobs_updated_at();

-- ============================================================
-- HELPER VIEWS
-- ============================================================

-- View: Pending batches that need to be collected
CREATE OR REPLACE VIEW pending_batches AS
SELECT
    id,
    run_id,
    site_key,
    stage,
    batch_id,
    article_count,
    submitted_at,
    EXTRACT(EPOCH FROM (NOW() - submitted_at)) / 60 AS minutes_waiting
FROM batch_jobs
WHERE status = 'pending'
ORDER BY submitted_at ASC;

-- View: Pipeline progress by run
CREATE OR REPLACE VIEW batch_pipeline_progress AS
SELECT
    run_id,
    site_key,
    MAX(CASE WHEN stage = 'research' THEN status END) AS research_status,
    MAX(CASE WHEN stage = 'planning' THEN status END) AS planning_status,
    MAX(CASE WHEN stage = 'write' THEN status END) AS write_status,
    MAX(CASE WHEN stage = 'qa' THEN status END) AS qa_status,
    SUM(article_count) AS total_articles,
    SUM(cost_cents) AS total_cost_cents,
    MIN(submitted_at) AS started_at,
    MAX(completed_at) AS last_completed_at
FROM batch_jobs
GROUP BY run_id, site_key
ORDER BY MIN(submitted_at) DESC;

-- ============================================================
-- EXAMPLE QUERIES
-- ============================================================

-- Get all pending batches ready for collection:
-- SELECT * FROM pending_batches;

-- Get pipeline progress for a specific run:
-- SELECT * FROM batch_pipeline_progress WHERE run_id = 'run_xxx';

-- Get all batches for a site:
-- SELECT * FROM batch_jobs WHERE site_key = 'lamphill' ORDER BY submitted_at DESC;

-- Mark a batch as completed:
-- UPDATE batch_jobs
-- SET status = 'completed', completed_at = NOW(), cost_cents = 50
-- WHERE batch_id = 'msgbatch_xxx';
