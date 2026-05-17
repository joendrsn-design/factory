-- Set next_run_at for all new sites so they get picked up by scheduler
UPDATE factory_registry
SET next_run_at = NOW()
WHERE next_run_at IS NULL
  AND status = 'active';

-- Verify
SELECT site_key, status, run_frequency, next_run_at
FROM factory_registry
WHERE status = 'active'
ORDER BY site_key;
