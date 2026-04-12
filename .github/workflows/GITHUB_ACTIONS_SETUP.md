# GitHub Actions Setup — Article Factory

## Overview

Four workflows automate the factory pipeline from the cloud:

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `factory-health-check.yml` | Daily 5am UTC + manual | Verify all API keys and endpoints before factory runs |
| `factory-scheduled.yml` | Daily 6am UTC + manual | Run all sites due per registry (realtime mode) |
| `factory-batch-submit.yml` | Manual only | Submit a high-volume batch job (50% cost savings) |
| `factory-batch-collect.yml` | Manual only | Collect completed batch and deposit to Site Empire |

---

## Prerequisites

Your factory code must be in a GitHub repository. If it isn't yet:

```bash
cd C:\Users\joend\projects\factory
git init
git add .
git commit -m "Initial factory commit"
gh repo create factory --private
git push -u origin main
```

Copy the `.github/` folder into the root of that repository:
```
factory/
├── .github/
│   └── workflows/
│       ├── factory-health-check.yml
│       ├── factory-scheduled.yml
│       ├── factory-batch-submit.yml
│       └── factory-batch-collect.yml
├── config/
├── pipeline/
├── orchestrator.py
└── ...
```

---

## Step 1 — Add GitHub Secrets

Go to your factory repo on GitHub:
**Settings → Secrets and variables → Actions → New repository secret**

Add all five:

| Secret Name | Value | Where to find it |
|-------------|-------|-----------------|
| `ANTHROPIC_API_KEY` | `sk-ant-...` | console.anthropic.com → API Keys |
| `FACTORY_API_KEY` | your shared secret | Same as Vercel + factory `.env` |
| `SITE_EMPIRE_URL` | `https://lamphill.org` | Your production domain |
| `SUPABASE_URL` | `https://xxx.supabase.co` | Supabase → Project Settings → API |
| `SUPABASE_SERVICE_KEY` | `eyJ...` | Supabase → Project Settings → API → service_role |

---

## Step 2 — Add pipeline/ to .gitignore

You don't want generated articles committed to the repo. Add to `.gitignore`:

```
# factory/.gitignore
.env
pipeline/topics/*
pipeline/research/*
pipeline/plans/*
pipeline/articles/*
pipeline/qa/*
!pipeline/topics/.gitkeep
!pipeline/research/.gitkeep
!pipeline/plans/.gitkeep
!pipeline/articles/.gitkeep
!pipeline/qa/.gitkeep
logs/
```

Create the `.gitkeep` files so the empty folders exist in git:
```bash
for dir in topics research plans articles qa; do
  touch pipeline/$dir/.gitkeep
done
```

---

## Step 3 — Verify Workflows Are Enabled

1. Go to your repo on GitHub
2. Click **Actions** tab
3. If prompted, click **"I understand my workflows, go ahead and enable them"**
4. You should see all four workflows listed

---

## Step 4 — Run Health Check First

Before the scheduled runs kick in:

1. GitHub → Actions → **Factory — Health Check**
2. Click **Run workflow** → **Run workflow**
3. Watch the logs — all three checks should pass green

If any check fails, fix it before proceeding.

---

## Daily Scheduled Behavior

**5:00 AM UTC** — Health check runs automatically
**6:00 AM UTC** — Factory runs automatically

The factory workflow calls `orchestrator.py due` which:
1. Queries `factory_registry` for sites where `next_run_at < now()`
2. Runs each due site in realtime mode
3. Updates `last_run_at` and `next_run_at` in registry
4. Deposits passing articles to Site Empire

No manual intervention needed once this is running.

---

## Manual Triggers

### Run a specific site immediately:
1. Actions → **Factory — Scheduled Run** → **Run workflow**
2. Fill in `site` (e.g. `lamphill`) and `count` (e.g. `2`)
3. Leave `dry_run` unchecked
4. Click **Run workflow**

### Large batch run (50% cheaper):
```
Step 1:  Run factory-batch-submit.yml
         site: lamphill
         count: 10
         stages: topics,research

Step 2:  Wait 15-30 minutes

Step 3:  Run factory-batch-collect.yml
         site: lamphill
         submit_run_id: [run ID from Step 1]
         stages: research,planning,write,qa,deposit
```

The run ID appears in the URL when you view the submit job:
`github.com/you/factory/actions/runs/12345678` → ID is `12345678`

---

## Viewing Results

**GitHub Actions logs** — Each run shows full output from every stage

**Pipeline artifacts** — On failure, the pipeline folder is uploaded as an artifact (3-day retention). On batch collect, final artifacts are kept 7 days.

**Site Empire admin** — After a successful run, articles appear at:
`https://lamphill.org/admin/queue`

**Registry status** — Every workflow prints `python registry.py list` at the end so you can see site health in the logs.

---

## Cost

GitHub Actions is **free for public repos** and **free up to 2,000 minutes/month for private repos**.

Each daily scheduled run costs approximately:
- Health check: ~1 minute
- Factory run (1 article, realtime): ~5-10 minutes
- Total: ~10-15 minutes/day × 30 days = ~400 minutes/month

Well within the free tier for a single site. At 10 sites running daily you'd use ~4,000 minutes — still on the free tier for public repos, or ~$1-2/month for private.

---

## Troubleshooting

**"Permission denied" on workflow_dispatch**
- You must have write access to the repo
- Workflows must be enabled under Actions tab

**"Secret not found"**
- Check secret names match exactly (case-sensitive)
- Secrets must be in the repo, not just organization

**Factory runs but no articles appear in Site Empire**
- Check `SITE_EMPIRE_URL` — should be `https://lamphill.org` not `http://localhost:3000`
- Check `FACTORY_API_KEY` matches Vercel's env var
- Look at Site Empire admin queue — articles may be in `pending_review`

**Orchestrator exits with "no sites due"**
- Check `factory_registry` table in Supabase
- Verify `next_run_at` is in the past for your site
- Run `python registry.py due` locally to debug
