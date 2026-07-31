#!/usr/bin/env python3
"""Validate every site config against the pipeline's SiteLoader rules.

Exits non-zero if any config (other than an explicitly allowlisted one) fails
validation. Wired into CI (.github/workflows/validate-configs.yml) so a config
can never silently regress to "born invalid" again — the generator side is
guarded by provision.py::_assert_config_valid.

Run locally:  python scripts/validate_configs.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Configs intentionally left invalid *for now*. Every entry is a known gap that
# CI is deliberately ignoring — keep this set empty in the steady state and
# remove a site as soon as its config is fixed.
KNOWN_INVALID = {
    "dailyseneca",  # no archived counterpart; awaiting authored voice values
}


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    config_dir = repo_root / "config" / "sites"

    # Ensure site_loader is importable regardless of the working directory.
    sys.path.insert(0, str(repo_root))
    from site_loader import SiteLoader  # noqa: E402

    loader = SiteLoader(config_dir=str(config_dir))
    site_ids = sorted(loader.list_sites())

    if not site_ids:
        print(f"No site configs found in {config_dir}", file=sys.stderr)
        return 1

    ok, skipped, failures = [], [], []
    for sid in site_ids:
        try:
            loader.load(sid, force_reload=True)
            ok.append(sid)
        except Exception as e:  # noqa: BLE001 - any load failure is a failure
            if sid in KNOWN_INVALID:
                skipped.append(sid)
            else:
                failures.append((sid, str(e).strip()))

    for sid in ok:
        print(f"  VALID    {sid}")
    for sid in skipped:
        print(f"  SKIPPED  {sid} (allowlisted known-invalid)")
    for sid, err in failures:
        first = err.splitlines()[0] if err else "invalid"
        print(f"  INVALID  {sid}: {first}")

    print(f"\n{len(ok)} valid, {len(skipped)} skipped, {len(failures)} invalid")

    # If an allowlisted config now passes, nudge to tighten the allowlist.
    stale = sorted(s for s in KNOWN_INVALID if s in ok)
    if stale:
        print(f"NOTE: allowlisted but now valid — remove from KNOWN_INVALID: {stale}")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
