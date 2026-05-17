#!/usr/bin/env python3
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
"""
============================================================
ARTICLE FACTORY — ORCHESTRATOR
============================================================
The brain. Chains all modules, handles the rewrite loop,
manages batch submission/collection, generates run reports.

Modes:
  REALTIME  — Run full pipeline synchronously (good for testing, small batches)
  BATCH     — Submit to Anthropic Batch API, collect later (50% savings, production)

Pipeline:
  TopicGen → Research → Planning → Write → QA → Deposit
                                    ↑              |
                                    └── REWRITE ───┘

Usage:
    # Full pipeline, one site, realtime
    python orchestrator.py run --site lamphill --count 3

    # Full pipeline, all sites, realtime
    python orchestrator.py run --all --count 5

    # Batch mode: submit phase
    python orchestrator.py submit --all

    # Batch mode: collect + continue pipeline
    python orchestrator.py collect

    # Just deposit what's ready
    python orchestrator.py deposit

    # Run report on current pipeline state
    python orchestrator.py status
============================================================
"""

import os
import sys
import json
import logging
import argparse
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import requests

from artifacts import (
    new_run_id, new_article_id,
    load_artifacts_from_dir, save_artifact, load_artifact,
    save_batch_manifest,
)
from site_loader import SiteLoader
from topic_generator import TopicGenerator
from research import ResearchModule
from planning import PlanningModule
from write import WriteModule
from qa import QAModule
from deposit import DepositEngine
from registry import Registry
from qa_failures import QAFailureTracker
from expansion import ExpansionModule
from preqa import PreQAModule
from category_tracker import CategoryTracker
from angle_bank import AngleBank

logger = logging.getLogger("article_factory.orchestrator")


# ── Batch Job Tracker ──────────────────────────────────────────

class BatchJobTracker:
    """
    Tracks batch jobs in Supabase batch_jobs table.
    Used for autonomous batch pipeline.
    """

    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not self.supabase_url or not self.supabase_key:
            logger.warning("Supabase credentials not found. BatchJobTracker in offline mode.")
            self._offline = True
        else:
            self._offline = False

    def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        """Make a request to Supabase REST API."""
        if self._offline:
            raise ConnectionError("BatchJobTracker is in offline mode")

        url = f"{self.supabase_url}/rest/v1/{endpoint}"
        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

        resp = requests.request(method, url, headers=headers, json=data, timeout=30)
        resp.raise_for_status()

        if resp.text:
            return resp.json()
        return {}

    def record_batch(
        self,
        run_id: str,
        site_key: str,
        stage: str,
        batch_id: str,
        article_count: int,
    ) -> bool:
        """Record a new batch job submission."""
        try:
            self._request("POST", "batch_jobs", {
                "run_id": run_id,
                "site_key": site_key,
                "stage": stage,
                "batch_id": batch_id,
                "status": "pending",
                "article_count": article_count,
            })
            logger.info(f"[batch_tracker] Recorded batch {batch_id} for {site_key}/{stage}")
            return True
        except Exception as e:
            logger.error(f"[batch_tracker] Failed to record batch: {e}")
            return False

    def get_pending_batches(self, stage: str = None) -> list[dict]:
        """Get all pending batch jobs, optionally filtered by stage."""
        try:
            endpoint = "batch_jobs?status=eq.pending&order=submitted_at.asc"
            if stage:
                endpoint += f"&stage=eq.{stage}"
            return self._request("GET", endpoint)
        except Exception as e:
            logger.error(f"[batch_tracker] Failed to get pending batches: {e}")
            return []

    def get_batches_for_run(self, run_id: str) -> list[dict]:
        """Get all batches for a specific run."""
        try:
            return self._request("GET", f"batch_jobs?run_id=eq.{run_id}&order=stage")
        except Exception as e:
            logger.error(f"[batch_tracker] Failed to get batches for run: {e}")
            return []

    def mark_completed(self, batch_id: str, cost_cents: int = 0) -> bool:
        """Mark a batch as completed."""
        try:
            self._request("PATCH", f"batch_jobs?batch_id=eq.{batch_id}", {
                "status": "completed",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "cost_cents": cost_cents,
            })
            logger.info(f"[batch_tracker] Marked {batch_id} as completed")
            return True
        except Exception as e:
            logger.error(f"[batch_tracker] Failed to mark completed: {e}")
            return False

    def mark_failed(self, batch_id: str, error_message: str) -> bool:
        """Mark a batch as failed."""
        try:
            self._request("PATCH", f"batch_jobs?batch_id=eq.{batch_id}", {
                "status": "failed",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "error_message": error_message[:500],
            })
            logger.info(f"[batch_tracker] Marked {batch_id} as failed")
            return True
        except Exception as e:
            logger.error(f"[batch_tracker] Failed to mark failed: {e}")
            return False

    def get_next_stage(self, run_id: str, site_key: str) -> Optional[str]:
        """
        Determine the next stage to process for a run/site.
        Returns None if all stages complete or still pending.
        """
        batches = self.get_batches_for_run(run_id)
        site_batches = [b for b in batches if b.get("site_key") == site_key]

        stages_order = ["research", "planning", "write", "qa"]
        completed_stages = {b["stage"] for b in site_batches if b.get("status") == "completed"}
        pending_stages = {b["stage"] for b in site_batches if b.get("status") == "pending"}

        # If any stage is still pending, wait
        if pending_stages:
            return None

        # Find the next stage after the last completed one
        for stage in stages_order:
            if stage not in completed_stages:
                return stage

        # All stages complete
        return None

    def is_run_complete(self, run_id: str, site_key: str) -> bool:
        """Check if all 4 stages are completed for a run/site."""
        batches = self.get_batches_for_run(run_id)
        site_batches = [b for b in batches if b.get("site_key") == site_key]

        if len(site_batches) < 4:
            return False

        return all(b.get("status") == "completed" for b in site_batches)

# Estimated cost per article in cents (for tracking)
COST_ESTIMATES = {
    "topic": 1,       # Haiku ~$0.01
    "research": 5,    # Sonnet ~$0.05
    "expansion": 1,   # Haiku ~$0.01
    "planning": 1,    # Haiku ~$0.01
    "write": 50,      # Opus ~$0.50
    "preqa": 1,       # Haiku ~$0.01 (sniff test saves expensive QA)
    "qa": 5,          # Sonnet ~$0.05
}


# ── Pipeline Directories ────────────────────────────────────

PIPELINE = {
    "topics":    "pipeline/topics",
    "research":  "pipeline/research",
    "angles":    "pipeline/angles",
    "plans":     "pipeline/plans",
    "articles":  "pipeline/articles",
    "qa":        "pipeline/qa",
    "batches":   "pipeline/batches",
    "reports":   "pipeline/reports",
    "rewrite":   "pipeline/rewrite",
}


def ensure_dirs():
    """Create all pipeline directories."""
    for d in PIPELINE.values():
        Path(d).mkdir(parents=True, exist_ok=True)


# ── Realtime Pipeline ───────────────────────────────────────

class RealtimePipeline:
    """
    Runs the full pipeline synchronously.
    Good for testing and small batches.
    Each module calls the API directly (no batching).
    """

    def __init__(self, config_dir: str = "config/sites"):
        self.config_dir = config_dir
        self.loader = SiteLoader(config_dir)
        self.topic_gen = TopicGenerator(config_dir=config_dir)
        self.research = ResearchModule(config_dir=config_dir)
        self.planning = PlanningModule(config_dir=config_dir)
        self.write = WriteModule(config_dir=config_dir)
        self.preqa = PreQAModule(config_dir=config_dir)
        self.qa = QAModule(config_dir=config_dir)
        self.deposit = DepositEngine(config_dir=config_dir)
        self.expansion = ExpansionModule(config_dir=config_dir)
        self.registry = Registry()
        self.failure_tracker = QAFailureTracker()
        self.category_tracker = CategoryTracker()
        self.angle_bank = AngleBank()

    def run_site(
        self,
        site_id: str,
        count: int = 3,
        article_type: str = "",
        run_id: str = "",
        max_rewrites: int = 2,
    ) -> dict:
        """
        Run full pipeline for one site.
        Returns run summary.
        """
        if not run_id:
            run_id = new_run_id()

        ensure_dirs()

        summary = {
            "run_id": run_id,
            "site_id": site_id,
            "started": datetime.now(timezone.utc).isoformat(),
            "topics_generated": 0,
            "researched": 0,
            "expanded": 0,
            "planned": 0,
            "written": 0,
            "preqa_failed": 0,  # Saved expensive QA calls
            "qa_passed": 0,
            "qa_rewrite": 0,
            "qa_killed": 0,
            "deposited": 0,
            "errors": [],
        }

        site_context = self.loader.load(site_id)
        site_max_rewrites = site_context.quality.get("max_rewrites", max_rewrites)

        # Check if expansion is enabled for this site
        expansion_config = site_context.raw_config.get("expansion", {})
        expansion_enabled = expansion_config.get("enabled", False)
        expansion_count = expansion_config.get("expansion_count", 3)

        # ── Get category priorities ─────────────────────────
        logger.info(f"[orchestrator] Analyzing category priorities for {site_id}")
        category_priorities = self.category_tracker.get_priorities(site_id, site_context)
        hungry_categories = category_priorities.hungry_categories
        saturated_categories = category_priorities.saturated_categories
        total_slots = category_priorities.total_slots_available

        logger.info(f"[orchestrator] Category analysis: {len(hungry_categories)} hungry, {len(saturated_categories)} saturated, {total_slots} slots available")

        # ── Step 0: Check Angle Bank ────────────────────────
        # Before generating new topics, see if we have banked angles for hungry categories
        banked_angles = []
        if expansion_enabled and hungry_categories:
            logger.info(f"[orchestrator] Step 0: Checking angle bank for {site_id}")
            banked_angles = self.angle_bank.withdraw(
                site_key=site_id,
                categories_needed=hungry_categories,
                count=count * expansion_count,  # Max we could use
                max_age_days=14,
            )
            if banked_angles:
                logger.info(f"[orchestrator] Found {len(banked_angles)} banked angles!")
                summary["banked_used"] = len(banked_angles)

        # Calculate how many new topics we need
        # If we have banked angles, reduce the number of new topics needed
        topics_needed = count
        if banked_angles:
            # Each topic produces expansion_count angles, so banked angles offset topics
            topics_offset = len(banked_angles) // expansion_count
            topics_needed = max(0, count - topics_offset)
            logger.info(f"[orchestrator] Banked angles offset {topics_offset} topics, need {topics_needed} new")

        # ── Step 1: Generate Topics (if needed) ─────────────
        topics = []
        if topics_needed > 0:
            logger.info(f"[orchestrator] Step 1: Generating {topics_needed} topics for {site_id}")
            topics = self.topic_gen.generate_for_site(
                site_id, count=topics_needed, article_type_filter=article_type, run_id=run_id
            )
            self.topic_gen.save_topics(topics, PIPELINE["topics"])
            summary["topics_generated"] = len(topics)
            logger.info(f"[orchestrator] Generated {len(topics)} topics")
        else:
            logger.info(f"[orchestrator] Step 1: Skipped topic generation (using banked angles)")
            summary["topics_generated"] = 0

        # ── Process banked angles first (they skip research) ──
        all_angles_to_process = []

        # Add banked angles to the processing queue
        for angle_meta, angle_body in banked_angles:
            # Check if this angle's category is still under quota
            cat = angle_meta.get("suggested_category", "")
            if cat in saturated_categories:
                logger.info(f"[orchestrator] Skipping banked angle (category {cat} saturated)")
                continue
            all_angles_to_process.append((angle_meta, angle_body))
            logger.info(f"[orchestrator] Using banked angle: {angle_meta.get('suggested_title', 'Unknown')[:50]}")

        # ── Steps 2-6: Process each topic through pipeline ──
        for topic_meta, topic_body in topics:
            article_id = topic_meta["article_id"]
            topic_name = topic_meta.get("topic", "Unknown")[:60]

            try:
                # Step 2: Research
                logger.info(f"[orchestrator] Step 2: Research — {topic_name}")
                res_meta, res_body = self.research.run_single(
                    topic_meta, topic_body, PIPELINE["research"]
                )
                summary["researched"] += 1

                # Step 2.5: Expansion (if enabled) — generates multiple angles from one research
                if expansion_enabled:
                    logger.info(f"[orchestrator] Step 2.5: Expansion — {topic_name}")
                    # Pass category priorities to guide angle generation
                    priority_dict = {
                        "hungry_categories": hungry_categories,
                        "saturated_categories": saturated_categories,
                        "category_scores": category_priorities.category_scores,
                    }
                    angles = self.expansion.run_expansion(
                        res_meta, res_body, PIPELINE["angles"],
                        category_priorities=priority_dict,
                    )
                    summary["expanded"] = summary.get("expanded", 0) + len(angles)
                    logger.info(f"[orchestrator] Generated {len(angles)} angles from research")

                    # Filter angles by quota - keep ones under quota, bank the rest
                    angles_to_use = []
                    angles_to_bank = []

                    for angle_meta, angle_body in angles:
                        cat = angle_meta.get("suggested_category", "")

                        if cat in saturated_categories:
                            # Category is at quota - bank for later
                            angles_to_bank.append((angle_meta, angle_body))
                            logger.info(f"[orchestrator] Banking angle (category {cat} saturated): {angle_meta.get('suggested_title', '')[:40]}")
                        else:
                            # Category has room - process now
                            angles_to_use.append((angle_meta, angle_body))

                    # Bank excess angles for future runs (saves research cost)
                    if angles_to_bank:
                        banked_count = self.angle_bank.deposit(site_id, angles_to_bank)
                        summary["banked"] = summary.get("banked", 0) + banked_count
                        logger.info(f"[orchestrator] Banked {banked_count} angles for future use")

                    # Add usable angles to processing queue
                    all_angles_to_process.extend(angles_to_use)
                else:
                    # No expansion: treat research as a single "angle"
                    all_angles_to_process.append((res_meta, res_body))

            except Exception as e:
                summary["errors"].append({
                    "article_id": article_id,
                    "topic": topic_name,
                    "error": str(e),
                })
                logger.error(f"[orchestrator] Error processing {topic_name}: {e}")

        # ── Process all angles through Planning → Write → QA ──
        logger.info(f"[orchestrator] Processing {len(all_angles_to_process)} total angles")

        for angle_idx, (angle_meta, angle_body) in enumerate(all_angles_to_process):
            angle_title = angle_meta.get("suggested_title", angle_meta.get("topic", "Unknown"))[:50]
            logger.info(f"[orchestrator] Processing angle {angle_idx + 1}/{len(all_angles_to_process)}: {angle_title}")

            try:
                # Step 3: Planning
                logger.info(f"[orchestrator] Step 3: Planning — {angle_title}")
                plan_meta, plan_body = self.planning.run_single(
                    angle_meta, angle_body, PIPELINE["plans"]
                )
                summary["planned"] += 1

                # Step 4-5: Write + QA (with rewrite loop)
                current_plan_meta = plan_meta
                current_plan_body = plan_body
                rewrite_count = 0

                while True:
                    # Step 4: Write
                    logger.info(f"[orchestrator] Step 4: Write — {angle_title} (attempt {rewrite_count + 1})")
                    art_meta, art_body = self.write.run_single(
                        current_plan_meta, current_plan_body, PIPELINE["articles"]
                    )
                    summary["written"] += 1

                    # Step 4.5: PreQA Sniff Test (cheap filter before expensive QA)
                    logger.info(f"[orchestrator] Step 4.5: PreQA sniff test — {angle_title}")
                    try:
                        preqa_meta, _ = self.preqa.run_single(art_meta, art_body)
                        preqa_verdict = preqa_meta.get("preqa_verdict", "PASS")
                        preqa_reason = preqa_meta.get("preqa_reason", "")

                        if preqa_verdict == "FAIL":
                            logger.info(f"[orchestrator] ⚡ PreQA FAIL: {preqa_reason} — skipping expensive QA")
                            summary["preqa_failed"] += 1
                            # Treat as rewrite needed without calling expensive QA
                            if rewrite_count < site_max_rewrites:
                                summary["qa_rewrite"] += 1
                                rewrite_count += 1
                                current_plan_meta = dict(plan_meta)
                                current_plan_meta["rewrite_count"] = rewrite_count
                                current_plan_meta["previous_feedback"] = f"PreQA failed: {preqa_reason}"
                                current_plan_body = plan_body + f"\n\n---\n\nPREQA ISSUE (attempt {rewrite_count}):\n{preqa_reason}\n\nFix the basic structural issue and try again."
                                continue  # Skip QA, go straight to rewrite
                            else:
                                logger.info(f"[orchestrator] ❌ PreQA failed and max rewrites exceeded")
                                # Record as failure
                                try:
                                    self.failure_tracker.record_failure(
                                        metadata=art_meta,
                                        body=art_body,
                                        verdict="REWRITE",
                                        score=0,
                                        scores_breakdown={},
                                        feedback=f"PreQA failed: {preqa_reason}",
                                        rewrite_instructions="Article failed basic structure checks",
                                        rewrite_count=rewrite_count,
                                    )
                                except Exception as fe:
                                    logger.warning(f"[orchestrator] Failed to record PreQA failure: {fe}")
                                break
                    except Exception as e:
                        # PreQA error = pass through to full QA (don't block on sniff test errors)
                        logger.warning(f"[orchestrator] PreQA error (passing through): {e}")

                    # Step 5: QA (only reached if PreQA passed)
                    logger.info(f"[orchestrator] Step 5: QA — {angle_title}")
                    art_meta["rewrite_count"] = rewrite_count
                    qa_meta, qa_body = self.qa.run_single(
                        art_meta, art_body, PIPELINE["qa"]
                    )

                    verdict = qa_meta.get("verdict", "KILL")

                    if verdict == "PUBLISH":
                        summary["qa_passed"] += 1
                        logger.info(f"[orchestrator] ✅ PUBLISH (score {qa_meta.get('score', '?')})")
                        logger.info(f"[orchestrator] Feedback: {qa_meta.get('feedback', '')[:300]}")
                        break

                    elif verdict == "REWRITE" and rewrite_count < site_max_rewrites:
                        summary["qa_rewrite"] += 1
                        rewrite_count += 1
                        logger.info(f"[orchestrator] 🔄 REWRITE {rewrite_count}/{site_max_rewrites} (score {qa_meta.get('score', '?')})")
                        logger.info(f"[orchestrator] Feedback: {qa_meta.get('feedback', '')[:300]}")
                        logger.info(f"[orchestrator] Rewrite instructions: {qa_meta.get('rewrite_instructions', '')[:300]}")

                        # Inject rewrite instructions into plan for next Write pass
                        current_plan_meta = dict(plan_meta)
                        current_plan_meta["rewrite_count"] = rewrite_count
                        current_plan_meta["previous_feedback"] = qa_meta.get("feedback", "")

                        # Append rewrite instructions to plan body
                        rewrite_inst = qa_meta.get("rewrite_instructions", "")
                        current_plan_body = plan_body + f"\n\n---\n\nREWRITE INSTRUCTIONS (attempt {rewrite_count}):\n{rewrite_inst}"
                        continue

                    else:
                        if verdict == "KILL":
                            summary["qa_killed"] += 1
                            logger.info(f"[orchestrator] ❌ KILL (score {qa_meta.get('score', '?')})")
                            logger.info(f"[orchestrator] Feedback: {qa_meta.get('feedback', '')[:500]}")
                        else:
                            summary["qa_rewrite"] += 1
                            logger.info(f"[orchestrator] ❌ Max rewrites exceeded, killing")
                            logger.info(f"[orchestrator] Feedback: {qa_meta.get('feedback', '')[:500]}")

                        # Record failure for admin review and recovery
                        try:
                            self.failure_tracker.record_failure(
                                metadata=art_meta,
                                body=art_body,
                                verdict=verdict if verdict == "KILL" else "REWRITE",
                                score=qa_meta.get("score", 0),
                                scores_breakdown=qa_meta.get("scores_breakdown"),
                                feedback=qa_meta.get("feedback", ""),
                                rewrite_instructions=qa_meta.get("rewrite_instructions", ""),
                                rewrite_count=rewrite_count,
                            )
                            logger.info(f"[orchestrator] 📝 Recorded failure for admin review")
                        except Exception as fe:
                            logger.warning(f"[orchestrator] Failed to record QA failure: {fe}")

                        break

            except Exception as e:
                angle_id = angle_meta.get("article_id", "unknown")
                summary["errors"].append({
                    "article_id": angle_id,
                    "topic": angle_title,
                    "error": str(e),
                })
                logger.error(f"[orchestrator] ❌ Error processing {angle_title}: {e}")

        # ── Step 6: Deposit ─────────────────────────────────
        logger.info(f"[orchestrator] Step 6: Deposit")
        dep_summary = self.deposit.deposit(
            input_dir=PIPELINE["qa"],
            site_filter=site_id,
            run_filter=run_id,
        )
        summary["deposited"] = len(dep_summary["published"])
        summary["quarantined"] = len(dep_summary.get("fallback_to_disk", []))
        summary["finished"] = datetime.now(timezone.utc).isoformat()

        # Calculate run duration and cost
        started = datetime.fromisoformat(summary["started"])
        finished = datetime.fromisoformat(summary["finished"])
        duration_seconds = int((finished - started).total_seconds())

        # Estimate cost (in cents)
        # Note: expansion cost is only charged once per research (not per angle)
        expansion_calls = summary["researched"] if expansion_enabled else 0
        # PreQA runs on all written articles, but saves expensive QA on failures
        preqa_calls = summary["written"]
        qa_calls = summary["qa_passed"] + summary["qa_rewrite"] + summary["qa_killed"]
        cost_cents = (
            summary["topics_generated"] * COST_ESTIMATES["topic"] +
            summary["researched"] * COST_ESTIMATES["research"] +
            expansion_calls * COST_ESTIMATES["expansion"] +
            summary["planned"] * COST_ESTIMATES["planning"] +
            summary["written"] * COST_ESTIMATES["write"] +
            preqa_calls * COST_ESTIMATES["preqa"] +
            qa_calls * COST_ESTIMATES["qa"]
        )
        # Track QA savings from PreQA
        summary["qa_saved_cents"] = summary["preqa_failed"] * COST_ESTIMATES["qa"]
        summary["cost_cents"] = cost_cents
        summary["duration_seconds"] = duration_seconds

        # Record run in registry
        try:
            status = "success" if not summary["errors"] else "partial"
            self.registry.record_run(
                site_key=site_id,
                run_id=run_id,
                status=status,
                articles_generated=summary["topics_generated"],
                articles_published=summary["deposited"],
                articles_rewritten=summary["qa_rewrite"],
                articles_killed=summary["qa_killed"],
                cost_cents=cost_cents,
                duration_seconds=duration_seconds,
                error_message=str(summary["errors"][0]) if summary["errors"] else None,
            )
        except Exception as e:
            logger.warning(f"[orchestrator] Failed to record run in registry: {e}")

        return summary

    def run_all(
        self,
        count_per_site: int = 3,
        run_id: str = "",
    ) -> dict:
        """Run pipeline for all configured sites."""
        if not run_id:
            run_id = new_run_id()

        all_summaries = {}
        for site_id in self.loader.list_sites():
            logger.info(f"\n{'='*60}\n[orchestrator] Processing site: {site_id}\n{'='*60}")
            summary = self.run_site(site_id, count=count_per_site, run_id=run_id)
            all_summaries[site_id] = summary

        return all_summaries


# ── Batch Pipeline ──────────────────────────────────────────

class BatchPipeline:
    """
    Batch mode: submit all work to Anthropic Batch API (50% off),
    then collect results later.

    Flow:
      submit_phase() → wait → collect_phase() → wait → submit_phase() ...
      Each call advances artifacts one stage through the pipeline.
    """

    def __init__(self, config_dir: str = "config/sites"):
        self.config_dir = config_dir
        self.loader = SiteLoader(config_dir)
        self.research = ResearchModule(config_dir=config_dir)
        self.planning = PlanningModule(config_dir=config_dir)
        self.write = WriteModule(config_dir=config_dir)
        self.qa = QAModule(config_dir=config_dir)

    def submit_stage(self, stage: str) -> Optional[str]:
        """
        Submit a pipeline stage as a batch.
        Reads from the stage's input folder, submits to Batch API.

        Returns batch_id or None.
        """
        ensure_dirs()

        module_map = {
            "research": (self.research, PIPELINE["topics"]),
            "planning": (self.planning, PIPELINE["research"]),
            "write":    (self.write, PIPELINE["plans"]),
            "qa":       (self.qa, PIPELINE["articles"]),
        }

        if stage not in module_map:
            logger.error(f"[batch] Unknown stage: {stage}")
            return None

        module, input_dir = module_map[stage]

        # Load unprocessed artifacts from input dir
        artifacts = load_artifacts_from_dir(input_dir)
        if not artifacts:
            logger.info(f"[batch] No artifacts in {input_dir}")
            return None

        # Filter to only artifacts that haven't been processed yet
        output_dir = PIPELINE[{
            "research": "research",
            "planning": "plans",
            "write": "articles",
            "qa": "qa",
        }[stage]]

        processed = set()
        for meta, _, _ in load_artifacts_from_dir(output_dir):
            processed.add(meta.get("article_id", ""))

        unprocessed = [
            (m, b, f) for m, b, f in artifacts
            if m.get("article_id", "") not in processed
        ]

        if not unprocessed:
            logger.info(f"[batch] All artifacts in {input_dir} already processed")
            return None

        logger.info(f"[batch] Submitting {len(unprocessed)} artifacts to {stage}")
        batch_id = module.submit(input_dir=input_dir)
        return batch_id

    def collect_stage(self, stage: str) -> int:
        """
        Collect batch results for a stage.
        Returns number of artifacts collected.
        """
        module_map = {
            "research": (self.research, PIPELINE["research"]),
            "planning": (self.planning, PIPELINE["plans"]),
            "write":    (self.write, PIPELINE["articles"]),
            "qa":       (self.qa, PIPELINE["qa"]),
        }

        if stage not in module_map:
            return 0

        module, output_dir = module_map[stage]
        return module.collect(output_dir=output_dir)

    def advance_all(self) -> dict:
        """
        Try to advance every stage.
        Collects any pending results, then submits new work.
        """
        results = {}
        for stage in ["research", "planning", "write", "qa"]:
            collected = self.collect_stage(stage)
            if collected:
                results[f"{stage}_collected"] = collected

            batch_id = self.submit_stage(stage)
            if batch_id:
                results[f"{stage}_submitted"] = batch_id

        return results


# ── Autonomous Batch Pipeline ──────────────────────────────────

class AutonomousBatchPipeline:
    """
    Fully autonomous batch pipeline for production.

    Phase 1 (6am): due --batch
      - Generate topics (realtime, Haiku, cheap)
      - Submit research/planning/write/qa to Batch API
      - Record batch IDs in Supabase batch_jobs table

    Phase 2 (9am): collect
      - Poll batch_jobs for pending batches
      - Collect completed batches from Anthropic API
      - For completed runs: deposit articles, update registry
    """

    def __init__(self, config_dir: str = "config/sites"):
        self.config_dir = config_dir
        self.loader = SiteLoader(config_dir)
        self.topic_gen = TopicGenerator(config_dir=config_dir)
        self.research = ResearchModule(config_dir=config_dir)
        self.planning = PlanningModule(config_dir=config_dir)
        self.write = WriteModule(config_dir=config_dir)
        self.qa = QAModule(config_dir=config_dir)
        self.deposit_engine = DepositEngine(config_dir=config_dir)
        self.registry = Registry()
        self.tracker = BatchJobTracker()

    def submit_due_sites(self, count_override: int = 0) -> dict:
        """
        Phase 1: Generate topics and submit all stages to Batch API.
        Called by 'orchestrator.py due --batch'.

        Returns summary of submissions.
        """
        ensure_dirs()

        due_sites = self.registry.get_due_sites()
        if not due_sites:
            logger.info("[autonomous] No sites due for a run")
            return {"sites": 0, "batches": []}

        logger.info(f"[autonomous] {len(due_sites)} sites due for batch processing")

        summary = {
            "sites": len(due_sites),
            "run_id": new_run_id(),
            "batches": [],
            "topics_generated": 0,
            "errors": [],
        }

        run_id = summary["run_id"]

        for site_status in due_sites:
            site_key = site_status.site_key
            count = count_override if count_override > 0 else site_status.articles_per_run

            logger.info(f"\n{'='*60}")
            logger.info(f"[autonomous] Processing {site_key} ({count} articles)")
            logger.info(f"{'='*60}")

            try:
                # Step 1: Generate topics (realtime - Haiku is cheap)
                logger.info(f"[autonomous] Step 1: Generating {count} topics")
                topics = self.topic_gen.generate_for_site(
                    site_key, count=count, run_id=run_id
                )
                self.topic_gen.save_topics(topics, PIPELINE["topics"])
                summary["topics_generated"] += len(topics)
                logger.info(f"[autonomous] Generated {len(topics)} topics")

                if not topics:
                    logger.warning(f"[autonomous] No topics generated for {site_key}")
                    continue

                # Step 2: Submit research batch
                logger.info(f"[autonomous] Step 2: Submitting research batch")
                research_batch_id = self._submit_stage_for_site(
                    "research", site_key, run_id, PIPELINE["topics"], len(topics)
                )
                if research_batch_id:
                    summary["batches"].append({
                        "site_key": site_key,
                        "stage": "research",
                        "batch_id": research_batch_id,
                        "article_count": len(topics),
                    })

            except Exception as e:
                logger.error(f"[autonomous] Error processing {site_key}: {e}")
                summary["errors"].append({"site_key": site_key, "error": str(e)})

        logger.info(f"\n[autonomous] Phase 1 complete:")
        logger.info(f"  Sites processed: {summary['sites']}")
        logger.info(f"  Topics generated: {summary['topics_generated']}")
        logger.info(f"  Batches submitted: {len(summary['batches'])}")

        return summary

    def _submit_stage_for_site(
        self,
        stage: str,
        site_key: str,
        run_id: str,
        input_dir: str,
        article_count: int,
    ) -> Optional[str]:
        """Submit a single stage to Batch API and track in Supabase."""
        import anthropic

        module_map = {
            "research": self.research,
            "planning": self.planning,
            "write": self.write,
            "qa": self.qa,
        }

        module = module_map.get(stage)
        if not module:
            return None

        # Load artifacts for this site/run
        artifacts = load_artifacts_from_dir(input_dir)
        site_artifacts = [
            (m, b, f) for m, b, f in artifacts
            if m.get("site_id") == site_key and m.get("run_id") == run_id
        ]

        if not site_artifacts:
            logger.warning(f"[autonomous] No artifacts for {site_key} in {input_dir}")
            return None

        # Build batch requests
        client = anthropic.Anthropic()
        requests_list = []

        for meta, body, filepath in site_artifacts:
            article_id = meta.get("article_id", "")
            site_context = self.loader.load(site_key)

            system_prompt, user_message = module.build_prompt(meta, body, site_context)

            requests_list.append({
                "custom_id": article_id,
                "params": {
                    "model": module.model,
                    "max_tokens": module.get_max_tokens(meta, site_context),
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_message}],
                }
            })

        if not requests_list:
            return None

        # Submit batch
        try:
            batch = client.batches.create(requests=requests_list)
            batch_id = batch.id
            logger.info(f"[autonomous] Submitted {stage} batch: {batch_id}")

            # Track in Supabase
            self.tracker.record_batch(
                run_id=run_id,
                site_key=site_key,
                stage=stage,
                batch_id=batch_id,
                article_count=len(requests_list),
            )

            # Also save local manifest for compatibility
            article_ids = [r["custom_id"] for r in requests_list]
            save_batch_manifest(batch_id, stage, article_ids, PIPELINE["batches"])

            return batch_id

        except Exception as e:
            logger.error(f"[autonomous] Failed to submit {stage} batch: {e}")
            return None

    def collect_and_advance(self) -> dict:
        """
        Phase 2: Collect completed batches, advance pipeline, deposit finished runs.
        Called by 'orchestrator.py collect'.

        Returns summary of operations.
        """
        import anthropic

        ensure_dirs()

        summary = {
            "batches_checked": 0,
            "batches_completed": 0,
            "batches_still_pending": 0,
            "stages_advanced": [],
            "runs_deposited": [],
            "errors": [],
        }

        # Get all pending batches
        pending = self.tracker.get_pending_batches()
        if not pending:
            logger.info("[autonomous] No pending batches to collect")
            return summary

        logger.info(f"[autonomous] Found {len(pending)} pending batches")
        summary["batches_checked"] = len(pending)

        client = anthropic.Anthropic()

        # Group by run_id/site_key for tracking progress
        runs_to_check = set()

        for job in pending:
            batch_id = job["batch_id"]
            stage = job["stage"]
            site_key = job["site_key"]
            run_id = job["run_id"]

            logger.info(f"[autonomous] Checking {stage} batch {batch_id} for {site_key}")

            try:
                batch = client.batches.retrieve(batch_id)

                if batch.processing_status == "ended":
                    logger.info(f"[autonomous] Batch {batch_id} completed!")

                    # Collect results
                    collected = self._collect_batch_results(
                        client, batch_id, stage, site_key, run_id
                    )

                    if collected > 0:
                        # Estimate cost
                        cost_cents = collected * COST_ESTIMATES.get(stage, 5)
                        self.tracker.mark_completed(batch_id, cost_cents)
                        summary["batches_completed"] += 1

                        # Track this run for potential next stage
                        runs_to_check.add((run_id, site_key))
                    else:
                        self.tracker.mark_failed(batch_id, "No results collected")
                        summary["errors"].append({
                            "batch_id": batch_id,
                            "error": "No results collected"
                        })

                elif batch.processing_status in ("in_progress", "created"):
                    logger.info(f"[autonomous] Batch {batch_id} still processing")
                    summary["batches_still_pending"] += 1

                else:
                    # Failed or cancelled
                    error_msg = f"Batch status: {batch.processing_status}"
                    self.tracker.mark_failed(batch_id, error_msg)
                    summary["errors"].append({"batch_id": batch_id, "error": error_msg})

            except Exception as e:
                logger.error(f"[autonomous] Error checking batch {batch_id}: {e}")
                summary["errors"].append({"batch_id": batch_id, "error": str(e)})

        # Advance runs that have completed stages
        for run_id, site_key in runs_to_check:
            next_stage = self.tracker.get_next_stage(run_id, site_key)

            if next_stage:
                logger.info(f"[autonomous] Advancing {site_key} to {next_stage}")

                # Determine input directory for next stage
                input_dirs = {
                    "research": PIPELINE["topics"],
                    "planning": PIPELINE["research"],
                    "write": PIPELINE["plans"],
                    "qa": PIPELINE["articles"],
                }

                # Count artifacts for this site/run
                artifacts = load_artifacts_from_dir(input_dirs[next_stage])
                site_artifacts = [
                    a for a in artifacts
                    if a[0].get("site_id") == site_key and a[0].get("run_id") == run_id
                ]

                if site_artifacts:
                    batch_id = self._submit_stage_for_site(
                        next_stage, site_key, run_id,
                        input_dirs[next_stage], len(site_artifacts)
                    )
                    if batch_id:
                        summary["stages_advanced"].append({
                            "site_key": site_key,
                            "stage": next_stage,
                            "batch_id": batch_id,
                        })

            elif self.tracker.is_run_complete(run_id, site_key):
                # All 4 stages complete - deposit!
                logger.info(f"[autonomous] Run {run_id} complete for {site_key}, depositing...")

                try:
                    dep_summary = self.deposit_engine.deposit(
                        input_dir=PIPELINE["qa"],
                        site_filter=site_key,
                        run_filter=run_id,
                    )

                    published = len(dep_summary.get("published", []))
                    logger.info(f"[autonomous] Deposited {published} articles for {site_key}")

                    # Update registry
                    self._finalize_run(run_id, site_key, published)

                    summary["runs_deposited"].append({
                        "run_id": run_id,
                        "site_key": site_key,
                        "published": published,
                    })

                except Exception as e:
                    logger.error(f"[autonomous] Deposit failed for {site_key}: {e}")
                    summary["errors"].append({
                        "run_id": run_id,
                        "site_key": site_key,
                        "error": str(e)
                    })

        logger.info(f"\n[autonomous] Phase 2 complete:")
        logger.info(f"  Batches completed: {summary['batches_completed']}")
        logger.info(f"  Still pending: {summary['batches_still_pending']}")
        logger.info(f"  Stages advanced: {len(summary['stages_advanced'])}")
        logger.info(f"  Runs deposited: {len(summary['runs_deposited'])}")

        return summary

    def _collect_batch_results(
        self,
        client,
        batch_id: str,
        stage: str,
        site_key: str,
        run_id: str,
    ) -> int:
        """Collect results from a completed batch and save artifacts."""
        module_map = {
            "research": (self.research, PIPELINE["topics"], PIPELINE["research"]),
            "planning": (self.planning, PIPELINE["research"], PIPELINE["plans"]),
            "write": (self.write, PIPELINE["plans"], PIPELINE["articles"]),
            "qa": (self.qa, PIPELINE["articles"], PIPELINE["qa"]),
        }

        module, input_dir, output_dir = module_map[stage]

        # Load original artifacts
        originals = load_artifacts_from_dir(input_dir)
        original_map = {
            m.get("article_id"): (m, b, f)
            for m, b, f in originals
            if m.get("site_id") == site_key and m.get("run_id") == run_id
        }

        collected = 0

        for result in client.batches.results(batch_id):
            article_id = result.custom_id

            if article_id not in original_map:
                logger.warning(f"[autonomous] No original found for {article_id}")
                continue

            input_meta, input_body, _ = original_map[article_id]
            site_context = self.loader.load(site_key)

            try:
                if result.result.type == "succeeded":
                    response_text = ""
                    for block in result.result.message.content:
                        if block.type == "text":
                            response_text += block.text

                    out_meta, out_body = module.parse_response(
                        response_text, input_meta, input_body, site_context
                    )

                    valid, error = module.validate_output(out_meta, out_body)
                    if valid:
                        save_artifact(out_meta, out_body, output_dir)
                        collected += 1
                        logger.info(f"[autonomous] Collected {article_id}")
                    else:
                        logger.warning(f"[autonomous] Validation failed for {article_id}: {error}")
                else:
                    logger.error(f"[autonomous] API error for {article_id}")

            except Exception as e:
                logger.error(f"[autonomous] Parse error for {article_id}: {e}")

        return collected

    def _finalize_run(self, run_id: str, site_key: str, published: int):
        """Update registry after a successful run."""
        # Calculate total cost from batch_jobs
        batches = self.tracker.get_batches_for_run(run_id)
        site_batches = [b for b in batches if b.get("site_key") == site_key]

        total_cost = sum(b.get("cost_cents", 0) for b in site_batches)
        total_articles = site_batches[0].get("article_count", 0) if site_batches else 0

        # Estimate duration (from first submit to now)
        if site_batches:
            first_submit = site_batches[0].get("submitted_at", "")
            if first_submit:
                try:
                    start = datetime.fromisoformat(first_submit.replace("Z", "+00:00"))
                    duration = int((datetime.now(timezone.utc) - start).total_seconds())
                except:
                    duration = 0
            else:
                duration = 0
        else:
            duration = 0

        try:
            self.registry.record_run(
                site_key=site_key,
                run_id=run_id,
                status="success",
                articles_generated=total_articles,
                articles_published=published,
                cost_cents=total_cost,
                duration_seconds=duration,
            )
            logger.info(f"[autonomous] Registry updated for {site_key}")
        except Exception as e:
            logger.warning(f"[autonomous] Failed to update registry: {e}")


# ── Status Report ───────────────────────────────────────────

def pipeline_status(config_dir: str = "config/sites") -> str:
    """Generate a status report of the current pipeline state."""
    lines = ["# Article Factory — Pipeline Status"]
    lines.append(f"\n**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    for stage, directory in PIPELINE.items():
        if stage in ("batches", "reports", "rewrite"):
            continue

        artifacts = load_artifacts_from_dir(directory)
        if not artifacts:
            lines.append(f"\n**{stage}:** 0 artifacts")
            continue

        # Count by site
        by_site = {}
        for meta, _, _ in artifacts:
            site_id = meta.get("site_id", "unknown")
            by_site[site_id] = by_site.get(site_id, 0) + 1

        total = len(artifacts)
        site_breakdown = ", ".join(f"{s}: {c}" for s, c in sorted(by_site.items()))
        lines.append(f"\n**{stage}:** {total} artifacts ({site_breakdown})")

        # Extra info for QA
        if stage == "qa":
            verdicts = {"PUBLISH": 0, "REWRITE": 0, "KILL": 0}
            for meta, _, _ in artifacts:
                v = meta.get("verdict", "")
                if v in verdicts:
                    verdicts[v] += 1
            lines.append(f"  Verdicts: PUBLISH={verdicts['PUBLISH']}, REWRITE={verdicts['REWRITE']}, KILL={verdicts['KILL']}")

    return "\n".join(lines)


# ── Run Summary Report ──────────────────────────────────────

def format_run_summary(summary: dict) -> str:
    """Format a single site run summary."""
    lines = [f"\n## {summary['site_id']}"]
    lines.append(f"Run ID: {summary['run_id']}")
    lines.append(f"Topics: {summary['topics_generated']} → Researched: {summary['researched']} → Planned: {summary['planned']}")
    preqa_failed = summary.get('preqa_failed', 0)
    if preqa_failed > 0:
        lines.append(f"Written: {summary['written']} → PreQA Failed: {preqa_failed} (saved {preqa_failed * 5}¢)")
    lines.append(f"Written: {summary['written']} → QA Passed: {summary['qa_passed']} | Rewrite: {summary['qa_rewrite']} | Killed: {summary['qa_killed']}")

    deposited = summary['deposited']
    quarantined = summary.get('quarantined', 0)
    if quarantined > 0:
        lines.append(f"Deposited: {deposited} | ⚠️  Quarantined (API failed): {quarantined}")
    else:
        lines.append(f"Deposited: {deposited}")

    if summary.get("errors"):
        lines.append(f"Errors: {len(summary['errors'])}")
        for err in summary["errors"]:
            lines.append(f"  - {err['topic']}: {err['error'][:100]}")

    return "\n".join(lines)


# ── CLI ─────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="Article Factory — Orchestrator")
    parser.add_argument("mode", choices=["run", "due", "submit", "collect", "deposit", "status", "advance"],
                       help="run=realtime, due=run sites due for a run, submit/collect=batch, deposit=final stage, status=report, advance=batch all stages")
    parser.add_argument("--site", default="", help="Specific site")
    parser.add_argument("--all", action="store_true", help="All sites")
    parser.add_argument("--count", type=int, default=0, help="Articles per site (0=use registry setting)")
    parser.add_argument("--type", default="", help="Filter article type")
    parser.add_argument("--stage", default="", help="Batch stage (research/planning/write/qa)")
    parser.add_argument("--batch", action="store_true", help="Use autonomous batch mode (for due command)")
    parser.add_argument("--dry-run", action="store_true", help="Preview deposit")
    parser.add_argument("--config", default="config/sites", help="Site config directory")
    parser.add_argument("--report", default="", help="Save report to file")

    args = parser.parse_args()

    if args.mode == "run":
        pipeline = RealtimePipeline(config_dir=args.config)

        if args.all:
            summaries = pipeline.run_all(count_per_site=args.count)
            report = "# Article Factory — Run Report\n"
            for site_id, summary in summaries.items():
                report += format_run_summary(summary)
        elif args.site:
            summary = pipeline.run_site(
                args.site, count=args.count, article_type=args.type
            )
            report = "# Article Factory — Run Report\n"
            report += format_run_summary(summary)
        else:
            print("Specify --site or --all")
            return

        print(report)
        if args.report:
            Path(args.report).parent.mkdir(parents=True, exist_ok=True)
            with open(args.report, "w") as f:
                f.write(report)

    elif args.mode == "due":
        if args.batch:
            # Autonomous batch mode: submit to Batch API, track in Supabase
            logger.info("[orchestrator] Running in autonomous batch mode")
            auto_pipeline = AutonomousBatchPipeline(config_dir=args.config)
            summary = auto_pipeline.submit_due_sites(count_override=args.count)

            report = "# Article Factory — Batch Submission Report\n\n"
            report += f"**Run ID:** {summary.get('run_id', 'N/A')}\n"
            report += f"**Sites processed:** {summary['sites']}\n"
            report += f"**Topics generated:** {summary['topics_generated']}\n"
            report += f"**Batches submitted:** {len(summary['batches'])}\n\n"

            if summary['batches']:
                report += "## Submitted Batches\n\n"
                report += "| Site | Stage | Batch ID | Articles |\n"
                report += "|------|-------|----------|----------|\n"
                for b in summary['batches']:
                    report += f"| {b['site_key']} | {b['stage']} | {b['batch_id'][:20]}... | {b['article_count']} |\n"

            if summary['errors']:
                report += "\n## Errors\n\n"
                for err in summary['errors']:
                    report += f"- **{err.get('site_key', 'Unknown')}:** {err.get('error', 'Unknown error')}\n"

            print(report)
            if args.report:
                Path(args.report).parent.mkdir(parents=True, exist_ok=True)
                with open(args.report, "w") as f:
                    f.write(report)

        else:
            # Original realtime mode
            pipeline = RealtimePipeline(config_dir=args.config)
            registry = Registry()

            due_sites = registry.get_due_sites()
            if not due_sites:
                print("No sites due for a run")
                return

            print(f"Sites due for a run: {len(due_sites)}")
            for site in due_sites:
                print(f"  - {site.site_key} ({site.articles_per_run} articles)")

            report = "# Article Factory — Scheduled Run Report\n"
            for site in due_sites:
                count = args.count if args.count > 0 else site.articles_per_run
                logger.info(f"\n{'='*60}\n[orchestrator] Running scheduled job: {site.site_key} ({count} articles)\n{'='*60}")
                try:
                    summary = pipeline.run_site(site.site_key, count=count, article_type=args.type)
                    report += format_run_summary(summary)
                except Exception as e:
                    logger.error(f"[orchestrator] Failed to run {site.site_key}: {e}")
                    registry.record_failure(site.site_key, new_run_id(), str(e))
                    report += f"\n## {site.site_key}\nFAILED: {e}\n"

            print(report)
            if args.report:
                Path(args.report).parent.mkdir(parents=True, exist_ok=True)
                with open(args.report, "w") as f:
                    f.write(report)

    elif args.mode == "submit":
        batch = BatchPipeline(config_dir=args.config)
        if args.stage:
            batch_id = batch.submit_stage(args.stage)
            print(f"Submitted: {batch_id}" if batch_id else "Nothing to submit")
        else:
            print("Specify --stage (research/planning/write/qa)")

    elif args.mode == "collect":
        if args.stage:
            # Legacy: collect specific stage
            batch = BatchPipeline(config_dir=args.config)
            count = batch.collect_stage(args.stage)
            print(f"Collected: {count} artifacts")
        else:
            # Autonomous mode: collect all pending, advance pipeline, deposit
            logger.info("[orchestrator] Running autonomous collect")
            auto_pipeline = AutonomousBatchPipeline(config_dir=args.config)
            summary = auto_pipeline.collect_and_advance()

            report = "# Article Factory — Batch Collection Report\n\n"
            report += f"**Batches checked:** {summary['batches_checked']}\n"
            report += f"**Batches completed:** {summary['batches_completed']}\n"
            report += f"**Still pending:** {summary['batches_still_pending']}\n\n"

            if summary['stages_advanced']:
                report += "## Stages Advanced\n\n"
                report += "| Site | Stage | Batch ID |\n"
                report += "|------|-------|----------|\n"
                for s in summary['stages_advanced']:
                    report += f"| {s['site_key']} | {s['stage']} | {s['batch_id'][:20]}... |\n"
                report += "\n"

            if summary['runs_deposited']:
                report += "## Runs Deposited\n\n"
                for r in summary['runs_deposited']:
                    report += f"- **{r['site_key']}:** {r['published']} articles published\n"
                report += "\n"

            if summary['errors']:
                report += "## Errors\n\n"
                for err in summary['errors']:
                    report += f"- {err}\n"

            print(report)
            if args.report:
                Path(args.report).parent.mkdir(parents=True, exist_ok=True)
                with open(args.report, "w") as f:
                    f.write(report)

    elif args.mode == "advance":
        batch = BatchPipeline(config_dir=args.config)
        results = batch.advance_all()
        for k, v in results.items():
            print(f"  {k}: {v}")

    elif args.mode == "deposit":
        engine = DepositEngine(config_dir=args.config)
        summary = engine.deposit(
            site_filter=args.site,
            dry_run=args.dry_run,
        )
        report = engine.generate_report(summary)
        print(report)

    elif args.mode == "status":
        print(pipeline_status(config_dir=args.config))


if __name__ == "__main__":
    main()