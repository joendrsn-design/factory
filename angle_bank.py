"""
============================================================
ARTICLE FACTORY — ANGLE BANK
============================================================
Stores excess angles for later use, saving research costs.

When expansion generates more angles than we can publish (due to
category quotas), we bank them for future runs.

Features:
  1. Bank angles - Save excess angles to disk
  2. Withdraw angles - Pull matching angles for categories that need content
  3. Age management - Expire angles older than max_age_days
  4. Category matching - Find banked angles for hungry categories

Storage: pipeline/angle_bank/{site_key}/*.md
Each angle is stored as a markdown artifact with frontmatter.
============================================================
"""

import os
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional

from artifacts import load_artifact, save_artifact, load_artifacts_from_dir

logger = logging.getLogger("article_factory.angle_bank")


class AngleBank:
    """
    Manages a bank of pre-generated angles for future use.

    Angles come from expansion module when we generate more than
    we can publish due to category quotas.

    Usage:
        bank = AngleBank()

        # Check bank before generating new research
        banked = bank.withdraw(site_key, categories_needed, count=3)

        # After expansion, bank extras
        bank.deposit(site_key, excess_angles)

        # Periodic cleanup
        bank.expire_old(site_key, max_age_days=14)
    """

    def __init__(self, bank_dir: str = "pipeline/angle_bank"):
        self.bank_dir = Path(bank_dir)
        self.bank_dir.mkdir(parents=True, exist_ok=True)

    def _site_dir(self, site_key: str) -> Path:
        """Get the bank directory for a site."""
        site_dir = self.bank_dir / site_key
        site_dir.mkdir(parents=True, exist_ok=True)
        return site_dir

    def deposit(
        self,
        site_key: str,
        angles: list[tuple[dict, str]],
    ) -> int:
        """
        Deposit angles into the bank for later use.

        Args:
            site_key: Site identifier
            angles: List of (metadata, body) tuples from expansion

        Returns: Number of angles deposited
        """
        if not angles:
            return 0

        site_dir = self._site_dir(site_key)
        deposited = 0

        for meta, body in angles:
            # Add banking metadata
            meta["banked_at"] = datetime.now(timezone.utc).isoformat()
            meta["bank_status"] = "available"

            # Generate filename
            article_id = meta.get("article_id", "unknown")
            filename = f"{article_id}_banked.md"

            try:
                save_artifact(meta, body, str(site_dir), filename)
                deposited += 1
                logger.info(f"[angle_bank] Deposited: {article_id} → {meta.get('suggested_category', '?')}")
            except Exception as e:
                logger.error(f"[angle_bank] Failed to deposit {article_id}: {e}")

        logger.info(f"[angle_bank] Deposited {deposited} angles for {site_key}")
        return deposited

    def withdraw(
        self,
        site_key: str,
        categories_needed: list[str],
        count: int = 5,
        max_age_days: int = 14,
    ) -> list[tuple[dict, str]]:
        """
        Withdraw angles from the bank matching category needs.

        Args:
            site_key: Site identifier
            categories_needed: Categories to prioritize (in order)
            count: Maximum angles to withdraw
            max_age_days: Don't use angles older than this

        Returns: List of (metadata, body) tuples
        """
        site_dir = self._site_dir(site_key)
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

        # Load all available angles
        available = []
        for filepath in site_dir.glob("*_banked.md"):
            try:
                meta, body = load_artifact(str(filepath))

                # Check if still available
                if meta.get("bank_status") != "available":
                    continue

                # Check age
                banked_at_str = meta.get("banked_at", "")
                if banked_at_str:
                    try:
                        banked_at = datetime.fromisoformat(banked_at_str.replace("Z", "+00:00"))
                        if banked_at < cutoff:
                            logger.debug(f"[angle_bank] Skipping expired: {filepath.name}")
                            continue
                    except:
                        pass

                available.append((meta, body, filepath))

            except Exception as e:
                logger.warning(f"[angle_bank] Failed to load {filepath}: {e}")

        if not available:
            logger.info(f"[angle_bank] No angles available for {site_key}")
            return []

        # Sort by category priority
        def category_priority(item):
            meta = item[0]
            cat = meta.get("suggested_category", "")
            try:
                return categories_needed.index(cat)
            except ValueError:
                return 999  # Unknown category goes last

        available.sort(key=category_priority)

        # Withdraw up to count
        withdrawn = []
        for meta, body, filepath in available[:count]:
            # Mark as withdrawn
            meta["bank_status"] = "withdrawn"
            meta["withdrawn_at"] = datetime.now(timezone.utc).isoformat()

            try:
                # Update the file to mark it withdrawn
                save_artifact(meta, body, str(site_dir), filepath.name)
                withdrawn.append((meta, body))

                logger.info(f"[angle_bank] Withdrew: {meta.get('article_id')} ({meta.get('suggested_category')})")

            except Exception as e:
                logger.error(f"[angle_bank] Failed to mark withdrawn: {e}")

        logger.info(f"[angle_bank] Withdrew {len(withdrawn)} angles for {site_key}")
        return withdrawn

    def get_inventory(
        self,
        site_key: str,
        max_age_days: int = 14,
    ) -> dict:
        """
        Get inventory of banked angles by category.

        Returns: {
            "total": int,
            "by_category": {category_slug: count},
            "oldest_days": int,
            "newest_days": int,
        }
        """
        site_dir = self._site_dir(site_key)
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        now = datetime.now(timezone.utc)

        by_category = {}
        ages = []

        for filepath in site_dir.glob("*_banked.md"):
            try:
                meta, _ = load_artifact(str(filepath))

                if meta.get("bank_status") != "available":
                    continue

                banked_at_str = meta.get("banked_at", "")
                if banked_at_str:
                    try:
                        banked_at = datetime.fromisoformat(banked_at_str.replace("Z", "+00:00"))
                        if banked_at < cutoff:
                            continue
                        age_days = (now - banked_at).days
                        ages.append(age_days)
                    except:
                        pass

                cat = meta.get("suggested_category", "uncategorized")
                by_category[cat] = by_category.get(cat, 0) + 1

            except:
                pass

        return {
            "total": sum(by_category.values()),
            "by_category": by_category,
            "oldest_days": max(ages) if ages else 0,
            "newest_days": min(ages) if ages else 0,
        }

    def expire_old(
        self,
        site_key: str,
        max_age_days: int = 14,
    ) -> int:
        """
        Mark old angles as expired.

        Returns: Number of angles expired
        """
        site_dir = self._site_dir(site_key)
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

        expired = 0

        for filepath in site_dir.glob("*_banked.md"):
            try:
                meta, body = load_artifact(str(filepath))

                if meta.get("bank_status") != "available":
                    continue

                banked_at_str = meta.get("banked_at", "")
                if not banked_at_str:
                    continue

                try:
                    banked_at = datetime.fromisoformat(banked_at_str.replace("Z", "+00:00"))
                except:
                    continue

                if banked_at < cutoff:
                    meta["bank_status"] = "expired"
                    meta["expired_at"] = datetime.now(timezone.utc).isoformat()
                    save_artifact(meta, body, str(site_dir), filepath.name)
                    expired += 1
                    logger.info(f"[angle_bank] Expired: {filepath.name}")

            except Exception as e:
                logger.warning(f"[angle_bank] Error processing {filepath}: {e}")

        if expired:
            logger.info(f"[angle_bank] Expired {expired} old angles for {site_key}")

        return expired

    def cleanup(
        self,
        site_key: str,
        delete_withdrawn: bool = True,
        delete_expired: bool = True,
    ) -> int:
        """
        Clean up withdrawn and expired angle files.

        Returns: Number of files deleted
        """
        site_dir = self._site_dir(site_key)
        deleted = 0

        for filepath in site_dir.glob("*_banked.md"):
            try:
                meta, _ = load_artifact(str(filepath))
                status = meta.get("bank_status", "")

                should_delete = (
                    (delete_withdrawn and status == "withdrawn") or
                    (delete_expired and status == "expired")
                )

                if should_delete:
                    filepath.unlink()
                    deleted += 1
                    logger.debug(f"[angle_bank] Deleted: {filepath.name}")

            except Exception as e:
                logger.warning(f"[angle_bank] Error cleaning {filepath}: {e}")

        if deleted:
            logger.info(f"[angle_bank] Cleaned up {deleted} files for {site_key}")

        return deleted

    def get_for_categories(
        self,
        site_key: str,
        category_slots: dict[str, int],
        max_age_days: int = 14,
    ) -> tuple[list[tuple[dict, str]], dict[str, int]]:
        """
        Get banked angles to fill category slots.

        Args:
            site_key: Site identifier
            category_slots: {category_slug: count_needed}
            max_age_days: Max age of angles to use

        Returns:
            (angles: list of (meta, body), remaining_slots: {cat: count_still_needed})
        """
        site_dir = self._site_dir(site_key)
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

        # Track what we've found
        found_by_cat = {cat: [] for cat in category_slots}
        remaining = dict(category_slots)

        # Load all available angles
        for filepath in site_dir.glob("*_banked.md"):
            try:
                meta, body = load_artifact(str(filepath))

                if meta.get("bank_status") != "available":
                    continue

                # Check age
                banked_at_str = meta.get("banked_at", "")
                if banked_at_str:
                    try:
                        banked_at = datetime.fromisoformat(banked_at_str.replace("Z", "+00:00"))
                        if banked_at < cutoff:
                            continue
                    except:
                        pass

                cat = meta.get("suggested_category", "")
                if cat in remaining and remaining[cat] > 0:
                    found_by_cat[cat].append((meta, body, filepath))
                    remaining[cat] -= 1

            except:
                pass

        # Withdraw the ones we're using
        withdrawn = []
        for cat, items in found_by_cat.items():
            for meta, body, filepath in items:
                meta["bank_status"] = "withdrawn"
                meta["withdrawn_at"] = datetime.now(timezone.utc).isoformat()

                try:
                    save_artifact(meta, body, str(site_dir), filepath.name)
                    withdrawn.append((meta, body))
                    logger.info(f"[angle_bank] Withdrew for {cat}: {meta.get('article_id')}")
                except Exception as e:
                    logger.error(f"[angle_bank] Failed to withdraw: {e}")
                    remaining[cat] = remaining.get(cat, 0) + 1

        return withdrawn, remaining


# ── CLI for testing ─────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Angle Bank")
    parser.add_argument("command", choices=["inventory", "expire", "cleanup"])
    parser.add_argument("--site", required=True, help="Site key")
    parser.add_argument("--max-age", type=int, default=14, help="Max age in days")

    args = parser.parse_args()

    bank = AngleBank()

    if args.command == "inventory":
        inv = bank.get_inventory(args.site, max_age_days=args.max_age)
        print(f"\nAngle Bank Inventory for {args.site}:")
        print("-" * 40)
        print(f"Total available: {inv['total']}")
        print(f"Age range: {inv['newest_days']}-{inv['oldest_days']} days")
        print("\nBy category:")
        for cat, count in sorted(inv['by_category'].items()):
            print(f"  {cat}: {count}")

    elif args.command == "expire":
        expired = bank.expire_old(args.site, max_age_days=args.max_age)
        print(f"Expired {expired} angles")

    elif args.command == "cleanup":
        deleted = bank.cleanup(args.site)
        print(f"Cleaned up {deleted} files")
