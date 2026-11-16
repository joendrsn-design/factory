"""
Reconcile publish_threshold between the site YAML configs and the DB.

Why: three stores previously disagreed on the publish threshold. The gate that
actually decides publishing reads sites.publish_threshold from the DB (seeded by
provision.py), NOT the YAML in config/sites/. The YAML is therefore easy to edit
with no runtime effect. This check flags any drift so the config file doesn't
silently become a fourth source of truth.

Compares, per site:
  - config/sites/<site>.yaml  -> quality.publish_threshold  (the file you edit)
  - sites table (by domain)   -> publish_threshold          (what P4 actually uses)

Exit code 0 if every YAML site has a matching DB threshold; 1 if any mismatch or
any YAML site is missing from the DB. Use it in CI/cron to catch drift early.

Run with: python scripts/reconcile_thresholds.py [--json]
"""
import argparse
import json
import logging
import sys
from pathlib import Path

import yaml

# Reuse the Factory's env-robust Supabase client (handles the several key names).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from p4_gates import get_supabase_client  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "sites"


def load_yaml_thresholds() -> dict[str, dict]:
    """Return {domain: {"site": <file stem>, "threshold": <float|None>}} from YAML configs."""
    out: dict[str, dict] = {}
    for path in sorted(CONFIG_DIR.glob("*.yaml")):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as e:
            logger.warning(f"  ! could not parse {path.name}: {e}")
            continue
        domain = data.get("domain")
        if not domain:
            logger.warning(f"  ! {path.name} has no domain, skipping")
            continue
        threshold = (data.get("quality") or {}).get("publish_threshold")
        out[domain] = {"site": path.stem, "threshold": threshold}
    return out


def load_db_thresholds() -> dict[str, float]:
    """Return {domain: publish_threshold} from the sites table."""
    db = get_supabase_client()
    result = db.table("sites").select("domain, publish_threshold").execute()
    return {row["domain"]: row.get("publish_threshold") for row in (result.data or [])}


def reconcile() -> dict:
    yaml_map = load_yaml_thresholds()
    db_map = load_db_thresholds()

    matches, mismatches, missing_in_db = [], [], []

    for domain, info in yaml_map.items():
        y = info["threshold"]
        if domain not in db_map:
            missing_in_db.append({"domain": domain, "site": info["site"], "yaml": y})
            continue
        d = db_map[domain]
        # Compare as floats; treat None explicitly.
        same = (y is not None and d is not None and float(y) == float(d))
        record = {"domain": domain, "site": info["site"], "yaml": y, "db": d}
        (matches if same else mismatches).append(record)

    return {
        "matches": matches,
        "mismatches": mismatches,
        "missing_in_db": missing_in_db,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile publish_threshold: YAML vs DB")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = parser.parse_args()

    report = reconcile()

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        logger.info(f"\npublish_threshold reconcile - {len(report['matches'])} OK, "
                    f"{len(report['mismatches'])} mismatched, "
                    f"{len(report['missing_in_db'])} missing in DB\n")
        for m in report["mismatches"]:
            logger.info(f"  MISMATCH  {m['domain']:<24} yaml={m['yaml']}  db={m['db']}  "
                        f"(P4 uses db={m['db']})")
        for m in report["missing_in_db"]:
            logger.info(f"  NO DB ROW {m['domain']:<24} yaml={m['yaml']}  "
                        f"(site not in sites table — P4 will use its 8.0 fallback)")
        if not report["mismatches"] and not report["missing_in_db"]:
            logger.info("  [OK] all site YAML thresholds match the DB")

    # Non-zero exit if there is drift, so this can gate CI/cron.
    return 1 if (report["mismatches"] or report["missing_in_db"]) else 0


if __name__ == "__main__":
    sys.exit(main())
