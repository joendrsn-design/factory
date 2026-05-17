-- QA Failures Storage
-- Stores articles that failed QA (KILL or exceeded max rewrites)
-- for admin review, editing, and republishing

CREATE TABLE IF NOT EXISTS qa_failures (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,

    -- Identification
    article_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    site_key TEXT NOT NULL,

    -- Verdict
    verdict TEXT NOT NULL,              -- REWRITE or KILL
    qa_score NUMERIC NOT NULL,
    scores_breakdown JSONB,
    feedback TEXT,
    rewrite_instructions TEXT,
    rewrite_count INTEGER DEFAULT 0,

    -- Article content (editable)
    title TEXT NOT NULL,
    slug TEXT,
    body TEXT NOT NULL,
    meta_description TEXT,
    category TEXT,
    tags JSONB DEFAULT '[]'::jsonb,
    sources JSONB DEFAULT '[]'::jsonb,

    -- Status
    status TEXT DEFAULT 'pending',      -- pending, editing, republished, archived
    reviewed_by TEXT,
    admin_notes TEXT,

    -- Timestamps
    failed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_qa_failures_status ON qa_failures(status);
CREATE INDEX IF NOT EXISTS idx_qa_failures_site ON qa_failures(site_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_qa_failures_article ON qa_failures(article_id);

-- Trigger to auto-update updated_at
CREATE OR REPLACE FUNCTION update_qa_failures_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS qa_failures_updated_at ON qa_failures;
CREATE TRIGGER qa_failures_updated_at
    BEFORE UPDATE ON qa_failures
    FOR EACH ROW
    EXECUTE FUNCTION update_qa_failures_updated_at();

-- Enable RLS (Row Level Security) if needed
ALTER TABLE qa_failures ENABLE ROW LEVEL SECURITY;

-- Policy for service role (full access)
CREATE POLICY "Service role has full access to qa_failures"
    ON qa_failures
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- Policy for authenticated users (read only)
CREATE POLICY "Authenticated users can read qa_failures"
    ON qa_failures
    FOR SELECT
    TO authenticated
    USING (true);
