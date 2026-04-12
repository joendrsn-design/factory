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

from artifacts import (
    new_run_id, new_article_id,
    load_artifacts_from_dir, save_artifact, load_artifact,
)
from site_loader import SiteLoader
from topic_generator import TopicGenerator
from research import ResearchModule
from planning import PlanningModule
from write import WriteModule
from qa import QAModule
from deposit import DepositEngine
from registry import Registry

logger = logging.getLogger("article_factory.orchestrator")

# Estimated cost per article in cents (for tracking)
COST_ESTIMATES = {
    "topic": 1,       # Haiku ~$0.01
    "research": 5,    # Sonnet ~$0.05
    "planning": 1,    # Haiku ~$0.01
    "write": 50,      # Opus ~$0.50
    "qa": 5,          # Sonnet ~$0.05
}


# ── Pipeline Directories ────────────────────────────────────

PIPELINE = {
    "topics":    "pipeline/topics",
    "research":  "pipeline/research",
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
        self.qa = QAModule(config_dir=config_dir)
        self.deposit = DepositEngine(config_dir=config_dir)
        self.registry = Registry()

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
            "planned": 0,
            "written": 0,
            "qa_passed": 0,
            "qa_rewrite": 0,
            "qa_killed": 0,
            "deposited": 0,
            "errors": [],
        }

        site_context = self.loader.load(site_id)
        site_max_rewrites = site_context.quality.get("max_rewrites", max_rewrites)

        # ── Step 1: Generate Topics ─────────────────────────
        logger.info(f"[orchestrator] Step 1: Generating {count} topics for {site_id}")
        topics = self.topic_gen.generate_for_site(
            site_id, count=count, article_type_filter=article_type, run_id=run_id
        )
        self.topic_gen.save_topics(topics, PIPELINE["topics"])
        summary["topics_generated"] = len(topics)
        logger.info(f"[orchestrator] Generated {len(topics)} topics")

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

                # Step 3: Planning
                logger.info(f"[orchestrator] Step 3: Planning — {topic_name}")
                plan_meta, plan_body = self.planning.run_single(
                    res_meta, res_body, PIPELINE["plans"]
                )
                summary["planned"] += 1

                # Step 4-5: Write + QA (with rewrite loop)
                current_plan_meta = plan_meta
                current_plan_body = plan_body
                rewrite_count = 0

                while True:
                    # Step 4: Write
                    logger.info(f"[orchestrator] Step 4: Write — {topic_name} (attempt {rewrite_count + 1})")
                    art_meta, art_body = self.write.run_single(
                        current_plan_meta, current_plan_body, PIPELINE["articles"]
                    )
                    summary["written"] += 1

                    # Step 5: QA
                    logger.info(f"[orchestrator] Step 5: QA — {topic_name}")
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
                        break

            except Exception as e:
                summary["errors"].append({
                    "article_id": article_id,
                    "topic": topic_name,
                    "error": str(e),
                })
                logger.error(f"[orchestrator] ❌ Error processing {topic_name}: {e}")

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
        cost_cents = (
            summary["topics_generated"] * COST_ESTIMATES["topic"] +
            summary["researched"] * COST_ESTIMATES["research"] +
            summary["planned"] * COST_ESTIMATES["planning"] +
            summary["written"] * COST_ESTIMATES["write"] +
            summary["qa_passed"] * COST_ESTIMATES["qa"] +
            summary["qa_rewrite"] * COST_ESTIMATES["qa"] +
            summary["qa_killed"] * COST_ESTIMATES["qa"]
        )
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
        # Run only sites that are due according to the registry
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
        batch = BatchPipeline(config_dir=args.config)
        if args.stage:
            count = batch.collect_stage(args.stage)
            print(f"Collected: {count} artifacts")
        else:
            print("Specify --stage (research/planning/write/qa)")

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