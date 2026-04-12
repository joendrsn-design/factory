"""
============================================================
ARTICLE FACTORY — BASE MODULE
============================================================
Abstract base class for all factory modules.
Batch-first, folder-decoupled architecture.

Every module supports two modes:
  python module.py submit   → reads input folder, builds batch, submits to Anthropic
  python module.py collect  → polls batch, parses results, deposits to output folder
  python module.py run      → real-time mode (one-by-one, no batch, for testing)

Modules are fully decoupled:
  - Each module reads from ONE input folder
  - Each module writes to ONE output folder
  - No chaining. No long-running connections.
  - If a module fails, everything upstream is safe in its folder.
============================================================
"""

import os
import json
import time
import logging
import argparse
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from site_loader import SiteLoader, SiteContext
from artifacts import (
    load_artifacts_from_dir, save_artifact, load_artifact,
    save_batch_manifest, find_latest_batch_manifest,
)

logger = logging.getLogger("article_factory")


class BaseModule(ABC):
    """
    Abstract base class. Every factory module inherits this.

    Subclass must define:
        module_name: str        — "research", "planning", "write", "qa"
        model: str              — Anthropic model string
        input_module: str       — which module's output to consume

    Subclass must implement:
        build_prompt(metadata, body, site_context) → (system_str, user_str)
        parse_response(response_text, input_metadata, input_body, site_context) → (out_metadata, out_body)
        validate_input(metadata, body) → (bool, str)
        validate_output(metadata, body) → (bool, str)
    """

    module_name: str = ""
    model: str = ""
    input_module: str = ""
    max_retries: int = 2
    default_max_tokens: int = 4096

    def __init__(self, config_dir: str = "config/sites"):
        self.loader = SiteLoader(config_dir=config_dir)
        if not self.module_name:
            raise ValueError("Subclass must define module_name")
        logger.info(f"[{self.module_name}] Initialized. Model: {self.model}")

    # ── Abstract Methods (subclass implements) ──────────────

    @abstractmethod
    def build_prompt(
        self,
        metadata: dict,
        body: str,
        site_context: SiteContext,
    ) -> tuple[str, str]:
        """
        Build prompts for the LLM.
        Returns (system_prompt, user_message).
        """
        pass

    @abstractmethod
    def parse_response(
        self,
        response_text: str,
        input_metadata: dict,
        input_body: str,
        site_context: SiteContext,
    ) -> tuple[dict, str]:
        """
        Parse LLM response into output artifact.
        Returns (output_metadata, output_body).
        """
        pass

    def validate_input(self, metadata: dict, body: str) -> tuple[bool, str]:
        """Override for module-specific input validation."""
        if metadata.get("status") != "complete":
            return False, f"Status is '{metadata.get('status')}', expected 'complete'"
        if not body.strip():
            return False, "Empty body"
        return True, ""

    def validate_output(self, metadata: dict, body: str) -> tuple[bool, str]:
        """Override for module-specific output validation."""
        if not body.strip():
            return False, "Empty output body"
        return True, ""

    def get_max_tokens(self, metadata: dict, site_context: SiteContext) -> int:
        """Override per module for token budget."""
        return self.default_max_tokens

    # ── SUBMIT: Build batch and send to Anthropic ───────────

    def submit(
        self,
        input_dir: str,
        batch_dir: str = "pipeline/batches",
        site_filter: str = "",
        run_filter: str = "",
    ) -> Optional[str]:
        """
        Read input folder, build batch request, submit to Anthropic.
        Returns batch_id or None if nothing to process.
        """
        import anthropic

        # Load input artifacts
        artifacts = load_artifacts_from_dir(
            input_dir,
            module_filter=self.input_module,
            status_filter="complete",
            site_filter=site_filter or None,
            run_filter=run_filter or None,
        )

        if not artifacts:
            logger.warning(f"[{self.module_name}] No artifacts found in {input_dir}")
            return None

        logger.info(f"[{self.module_name}] Building batch for {len(artifacts)} artifacts")

        # Build batch requests
        requests = []
        article_ids = []

        for metadata, body, filepath in artifacts:
            # Validate input
            valid, error = self.validate_input(metadata, body)
            if not valid:
                logger.warning(f"[{self.module_name}] Skipping {metadata.get('article_id')}: {error}")
                continue

            # Load site context
            site_id = metadata.get("site_id", "")
            try:
                site_context = self.loader.load(site_id)
            except Exception as e:
                logger.error(f"[{self.module_name}] Failed to load config for '{site_id}': {e}")
                continue

            # Build prompts
            system_prompt, user_message = self.build_prompt(metadata, body, site_context)

            article_id = metadata.get("article_id", "unknown")
            article_ids.append(article_id)

            requests.append({
                "custom_id": article_id,
                "params": {
                    "model": self.model,
                    "max_tokens": self.get_max_tokens(metadata, site_context),
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_message}],
                },
            })

        if not requests:
            logger.warning(f"[{self.module_name}] No valid artifacts to process")
            return None

        # Submit batch
        client = anthropic.Anthropic()
        logger.info(f"[{self.module_name}] Submitting batch of {len(requests)} requests...")

        # Write requests to JSONL for batch API
        jsonl_path = Path(batch_dir) / f"{self.module_name}_requests.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)

        with open(jsonl_path, "w") as f:
            for req in requests:
                f.write(json.dumps(req) + "\n")

        # Submit to Anthropic Batch API
        batch = client.batches.create(
            requests=requests,
        )

        batch_id = batch.id
        logger.info(f"[{self.module_name}] ✅ Batch submitted: {batch_id}")

        # Save manifest for collect step
        save_batch_manifest(batch_id, self.module_name, article_ids, batch_dir)

        return batch_id

    # ── COLLECT: Poll batch and deposit results ─────────────

    def collect(
        self,
        input_dir: str,
        output_dir: str,
        batch_dir: str = "pipeline/batches",
        batch_id: str = "",
        poll_interval: int = 30,
        max_poll_time: int = 3600,
    ) -> list[dict]:
        """
        Poll for batch completion, parse results, deposit to output folder.
        Returns list of result summaries.
        """
        import anthropic

        # Find the batch to collect
        if not batch_id:
            manifest = find_latest_batch_manifest(batch_dir, module=self.module_name)
            if not manifest:
                logger.error(f"[{self.module_name}] No batch manifest found in {batch_dir}")
                return []
            batch_id = manifest["batch_id"]

        client = anthropic.Anthropic()

        # Poll for completion
        logger.info(f"[{self.module_name}] Polling batch {batch_id}...")
        start_time = time.time()

        while True:
            batch = client.batches.retrieve(batch_id)

            if batch.processing_status == "ended":
                logger.info(f"[{self.module_name}] Batch complete!")
                break

            elapsed = time.time() - start_time
            if elapsed > max_poll_time:
                logger.error(f"[{self.module_name}] Batch timed out after {max_poll_time}s")
                return []

            logger.info(
                f"[{self.module_name}] Batch still processing... "
                f"({int(elapsed)}s elapsed, checking again in {poll_interval}s)"
            )
            time.sleep(poll_interval)

        # Load original artifacts for context
        originals = load_artifacts_from_dir(input_dir, module_filter=self.input_module)
        original_map = {}
        for meta, body, fpath in originals:
            original_map[meta.get("article_id")] = (meta, body, fpath)

        # Process results
        results_summary = []

        for result in client.batches.results(batch_id):
            article_id = result.custom_id

            original = original_map.get(article_id)
            if not original:
                logger.error(f"[{self.module_name}] No original found for {article_id}")
                continue

            input_meta, input_body, input_path = original
            site_context = self.loader.load(input_meta.get("site_id", ""))

            try:
                if result.result.type == "succeeded":
                    # Extract text from response
                    response_text = ""
                    for block in result.result.message.content:
                        if block.type == "text":
                            response_text += block.text

                    # Parse into output artifact
                    out_meta, out_body = self.parse_response(
                        response_text, input_meta, input_body, site_context
                    )

                    # Validate output
                    valid, error = self.validate_output(out_meta, out_body)
                    if valid:
                        save_artifact(out_meta, out_body, output_dir)
                        results_summary.append({
                            "article_id": article_id, "status": "success",
                        })
                        logger.info(f"[{self.module_name}] ✅ {article_id}")
                    else:
                        out_meta["status"] = "failed"
                        out_meta["error"] = error
                        save_artifact(out_meta, out_body, output_dir)
                        results_summary.append({
                            "article_id": article_id, "status": "validation_failed", "error": error,
                        })
                        logger.warning(f"[{self.module_name}] ❌ {article_id}: {error}")
                else:
                    error_msg = str(getattr(result.result, 'error', 'Unknown error'))
                    results_summary.append({
                        "article_id": article_id, "status": "api_error", "error": error_msg,
                    })
                    logger.error(f"[{self.module_name}] ❌ {article_id}: {error_msg}")

            except Exception as e:
                results_summary.append({
                    "article_id": article_id, "status": "parse_error", "error": str(e),
                })
                logger.error(f"[{self.module_name}] ❌ {article_id}: {e}")

        # Summary
        successes = sum(1 for r in results_summary if r["status"] == "success")
        failures = len(results_summary) - successes
        logger.info(f"[{self.module_name}] Collect complete: {successes} succeeded, {failures} failed")

        return results_summary

    # ── RUN: Real-time mode (no batch, for testing) ─────────

    def run_realtime(
        self,
        input_dir: str,
        output_dir: str,
        site_filter: str = "",
        limit: int = 0,
    ) -> list[dict]:
        """
        Process artifacts one-by-one with direct API calls.
        Use for testing or small runs. Not cost-optimized.
        """
        import anthropic

        artifacts = load_artifacts_from_dir(
            input_dir,
            module_filter=self.input_module,
            status_filter="complete",
            site_filter=site_filter or None,
        )

        if limit:
            artifacts = artifacts[:limit]

        if not artifacts:
            logger.warning(f"[{self.module_name}] No artifacts found")
            return []

        client = anthropic.Anthropic()
        results = []

        for metadata, body, filepath in artifacts:
            article_id = metadata.get("article_id", "unknown")

            valid, error = self.validate_input(metadata, body)
            if not valid:
                logger.warning(f"[{self.module_name}] Skipping {article_id}: {error}")
                results.append({"article_id": article_id, "status": "skipped", "error": error})
                continue

            site_context = self.loader.load(metadata.get("site_id", ""))
            system_prompt, user_message = self.build_prompt(metadata, body, site_context)

            for attempt in range(1, self.max_retries + 1):
                try:
                    response = client.messages.create(
                        model=self.model,
                        max_tokens=self.get_max_tokens(metadata, site_context),
                        system=system_prompt,
                        messages=[{"role": "user", "content": user_message}],
                    )

                    response_text = ""
                    for block in response.content:
                        if block.type == "text":
                            response_text += block.text

                    out_meta, out_body = self.parse_response(
                        response_text, metadata, body, site_context
                    )

                    valid, error = self.validate_output(out_meta, out_body)
                    if valid:
                        save_artifact(out_meta, out_body, output_dir)
                        results.append({"article_id": article_id, "status": "success"})
                        logger.info(f"[{self.module_name}] ✅ {article_id}")
                        break
                    else:
                        logger.warning(f"[{self.module_name}] Validation failed attempt {attempt}: {error}")
                        if attempt == self.max_retries:
                            results.append({"article_id": article_id, "status": "failed", "error": error})

                except Exception as e:
                    logger.error(f"[{self.module_name}] Error attempt {attempt}: {e}")
                    if attempt == self.max_retries:
                        results.append({"article_id": article_id, "status": "failed", "error": str(e)})
                    else:
                        time.sleep(2 ** attempt)

        successes = sum(1 for r in results if r["status"] == "success")
        logger.info(f"[{self.module_name}] Run complete: {successes}/{len(results)} succeeded")
        return results

    def run_single(
        self,
        metadata: dict,
        body: str,
        output_dir: str = "",
    ) -> tuple[dict, str]:
        """
        Process a single artifact through this module (realtime, no batching).
        Used by the orchestrator to chain modules.

        Returns (output_metadata, output_body).
        Raises on failure after retries.
        """
        import anthropic

        valid, error = self.validate_input(metadata, body)
        if not valid:
            raise ValueError(f"[{self.module_name}] Input validation failed: {error}")

        site_context = self.loader.load(metadata.get("site_id", ""))
        system_prompt, user_message = self.build_prompt(metadata, body, site_context)

        client = anthropic.Anthropic()

        for attempt in range(1, self.max_retries + 1):
            try:
                response = client.messages.create(
                    model=self.model,
                    max_tokens=self.get_max_tokens(metadata, site_context),
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_message}],
                )

                response_text = ""
                for block in response.content:
                    if block.type == "text":
                        response_text += block.text

                out_meta, out_body = self.parse_response(
                    response_text, metadata, body, site_context
                )

                valid, error = self.validate_output(out_meta, out_body)
                if valid:
                    if output_dir:
                        save_artifact(out_meta, out_body, output_dir)
                    logger.info(f"[{self.module_name}] ✅ {metadata.get('article_id', 'unknown')}")
                    return out_meta, out_body
                else:
                    logger.warning(f"[{self.module_name}] Output validation attempt {attempt}: {error}")
                    if attempt == self.max_retries:
                        raise ValueError(f"Output validation failed: {error}")

            except anthropic.APIError as e:
                logger.error(f"[{self.module_name}] API error attempt {attempt}: {e}")
                if attempt == self.max_retries:
                    raise
                time.sleep(2 ** attempt)

        raise RuntimeError(f"[{self.module_name}] Failed after {self.max_retries} attempts")

    # ── CLI Entrypoint ──────────────────────────────────────

    @classmethod
    def cli(cls, config_dir: str = "config/sites"):
        """
        Standard CLI for any module. Call from if __name__ == "__main__".

        Usage:
            python planning.py submit --input pipeline/research
            python planning.py collect --input pipeline/research --output pipeline/plans
            python planning.py run --input pipeline/research --output pipeline/plans
        """
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )

        parser = argparse.ArgumentParser(description=f"Article Factory — {cls.module_name}")
        parser.add_argument("mode", choices=["submit", "collect", "run"],
                          help="submit=batch submit, collect=batch collect, run=realtime")
        parser.add_argument("--input", default=f"pipeline/{cls.input_module}",
                          help="Input directory")
        parser.add_argument("--output", default=f"pipeline/{cls.module_name}",
                          help="Output directory")
        parser.add_argument("--batch-dir", default="pipeline/batches",
                          help="Batch manifest directory")
        parser.add_argument("--config", default=config_dir,
                          help="Site config directory")
        parser.add_argument("--site", default="", help="Filter by site_id")
        parser.add_argument("--run-id", default="", help="Filter by run_id")
        parser.add_argument("--batch-id", default="", help="Specific batch to collect")
        parser.add_argument("--limit", type=int, default=0,
                          help="Max artifacts to process (realtime mode)")
        parser.add_argument("--poll-interval", type=int, default=30,
                          help="Seconds between batch polls")

        args = parser.parse_args()
        module = cls(config_dir=args.config)

        if args.mode == "submit":
            batch_id = module.submit(
                input_dir=args.input,
                batch_dir=args.batch_dir,
                site_filter=args.site,
                run_filter=args.run_id,
            )
            if batch_id:
                print(f"\n✅ Batch submitted: {batch_id}")
                print(f"   Run: python {cls.module_name}.py collect --input {args.input} --output {args.output}")
            else:
                print("\n⚠️  Nothing to submit")

        elif args.mode == "collect":
            results = module.collect(
                input_dir=args.input,
                output_dir=args.output,
                batch_dir=args.batch_dir,
                batch_id=args.batch_id,
                poll_interval=args.poll_interval,
            )
            successes = sum(1 for r in results if r["status"] == "success")
            print(f"\n{'='*50}")
            print(f"Collect complete: {successes}/{len(results)} succeeded")
            print(f"Output: {args.output}")
            print(f"{'='*50}")

        elif args.mode == "run":
            results = module.run_realtime(
                input_dir=args.input,
                output_dir=args.output,
                site_filter=args.site,
                limit=args.limit,
            )
            successes = sum(1 for r in results if r["status"] == "success")
            print(f"\n{'='*50}")
            print(f"Run complete: {successes}/{len(results)} succeeded")
            print(f"{'='*50}")
