#!/usr/bin/env python3
"""
provision.py - Site provisioning CLI for Article Factory + Site Empire

Usage:
    python provision.py new --site mysite --domain mysite.com --niche health-longevity
    python provision.py new --site mysite --domain mysite.com --niche finance --template minimal-daily
    python provision.py list
    python provision.py status --site mysite
    python provision.py deactivate --site mysite

Options:
    --site          Site ID / slug (lowercase, hyphens ok, e.g. "lamp-hill")
    --domain        Primary domain (e.g. "lamphill.org")
    --niche         Content niche (e.g. "health-longevity", "finance", "fitness")
    --template      Template slug (default: "minimal-daily")
    --tier          Site tier: flagship | standard | micro (default: standard)
    --name          Display name (default: title-cased from --site)
    --dry-run       Show what would happen without doing it
"""

import argparse
import os
import sys
import json
import secrets
import textwrap
from datetime import datetime
from pathlib import Path

# ── Optional deps (graceful degradation) ──────────────────────────────────────
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

try:
    from dotenv import load_dotenv
    load_dotenv()
    HAS_DOTENV = True
except ImportError:
    HAS_DOTENV = False

try:
    from supabase import create_client
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False

# ── Paths ──────────────────────────────────────────────────────────────────────
FACTORY_ROOT  = Path(__file__).parent
SITES_DIR     = FACTORY_ROOT / "config" / "sites"
ASSETS_HINT   = Path("site-empire") / "public" / "sites"   # relative hint for output

# ── Niche → category presets ───────────────────────────────────────────────────
NICHE_PRESETS = {
    "health-longevity": {
        "categories": [
            {"slug": "supplements"},
            {"slug": "fitness"},
            {"slug": "nutrition"},
            {"slug": "sleep"},
            {"slug": "labs"},
            {"slug": "endocrine"},
            {"slug": "hot-cold"},
            {"slug": "leading-voices"},
            {"slug": "events"},
        ],
        "voice": {
            "tone": "authoritative yet approachable",
            "persona": "physician-researcher translating evidence",
        },
        "publish_threshold": 8.0,
    },
    "finance": {
        "categories": [
            {"slug": "investing"},
            {"slug": "trading"},
            {"slug": "tax"},
            {"slug": "real-estate"},
            {"slug": "crypto"},
            {"slug": "retirement"},
            {"slug": "news"},
        ],
        "voice": {
            "tone": "clear, direct, no hype",
            "persona": "independent analyst cutting through noise",
        },
        "publish_threshold": 8.0,
    },
    "fitness": {
        "categories": [
            {"slug": "training"},
            {"slug": "nutrition"},
            {"slug": "recovery"},
            {"slug": "gear"},
            {"slug": "mindset"},
            {"slug": "programs"},
        ],
        "voice": {
            "tone": "motivating but evidence-based",
            "persona": "experienced coach who reads the research",
        },
        "publish_threshold": 7.5,
    },
    "technology": {
        "categories": [
            {"slug": "ai"},
            {"slug": "software"},
            {"slug": "hardware"},
            {"slug": "startups"},
            {"slug": "security"},
            {"slug": "tutorials"},
        ],
        "voice": {
            "tone": "technically precise, jargon where earned",
            "persona": "senior engineer who explains things well",
        },
        "publish_threshold": 8.0,
    },
    "personal-development": {
        "categories": [
            {"slug": "habits"},
            {"slug": "productivity"},
            {"slug": "mindset"},
            {"slug": "relationships"},
            {"slug": "career"},
            {"slug": "learning"},
        ],
        "voice": {
            "tone": "honest, direct, no toxic positivity",
            "persona": "thoughtful practitioner sharing what actually works",
        },
        "publish_threshold": 7.5,
    },
    "trading": {
        "categories": [
            {"slug": "the-strat"},
            {"slug": "setups"},
            {"slug": "futures"},
            {"slug": "stocks"},
            {"slug": "options"},
            {"slug": "risk-management"},
            {"slug": "psychology"},
            {"slug": "market-structure"},
            {"slug": "tools"},
            {"slug": "education"},
        ],
        "voice": {
            "tone": "direct, no hype, practitioner-first",
            "persona": "active trader who has logged the screen time and shows the work",
        },
        "publish_threshold": 8.0,
    },
}

VALID_TEMPLATES = ["minimal-daily", "lamphill", "minimal-blog"]
VALID_TIERS     = ["flagship", "standard", "micro"]

# ── Helpers ────────────────────────────────────────────────────────────────────

def title_case(slug: str) -> str:
    return " ".join(w.capitalize() for w in slug.replace("-", " ").replace("_", " ").split())

def validate_site_id(site_id: str) -> None:
    import re
    if not re.match(r'^[a-z0-9][a-z0-9\-]{1,48}[a-z0-9]$', site_id):
        die(f"Invalid site ID '{site_id}'. Use lowercase letters, numbers, hyphens. 3-50 chars.")

def validate_domain(domain: str) -> None:
    import re
    if not re.match(r'^[a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?(\.[a-z]{2,})+$', domain.lower()):
        die(f"Invalid domain '{domain}'.")

def die(msg: str) -> None:
    print(f"\n❌  {msg}\n", file=sys.stderr)
    sys.exit(1)

def ok(msg: str) -> None:
    print(f"  ✅  {msg}")

def warn(msg: str) -> None:
    print(f"  ⚠️   {msg}")

def info(msg: str) -> None:
    print(f"  ℹ️   {msg}")

def section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")

def get_supabase():
    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL") or os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        return None
    if not HAS_SUPABASE:
        return None
    return create_client(url, key)

# ── YAML generation ────────────────────────────────────────────────────────────

def build_site_yaml(args, preset: dict) -> str:
    name = args.name or title_case(args.site)
    data = {
        "site_id":   args.site,
        "domain":    args.domain,
        "name":      name,
        "niche":     args.niche,
        "tier":      args.tier,
        "template":  args.template,

        "categories": preset["categories"],

        "voice": preset["voice"],

        "article_types": {
            "deep_dive": {
                "word_count": [2500, 3500],
                "frequency":  "weekly",
            },
            "listicle": {
                "word_count": [1500, 2000],
                "frequency":  "biweekly",
            },
        },

        "qa": {
            "min_score":         7.0,
            "publish_threshold": preset["publish_threshold"],
        },

        "meta": {
            "created_at":    datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "provisioned_by": "provision.py",
        },
    }

    if HAS_YAML:
        return yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=False)

    # Fallback: hand-roll minimal YAML
    lines = [
        f"site_id: {data['site_id']}",
        f"domain: {data['domain']}",
        f"name: \"{data['name']}\"",
        f"niche: {data['niche']}",
        f"tier: {data['tier']}",
        f"template: {data['template']}",
        "",
        "categories:",
    ]
    for cat in data["categories"]:
        lines.append(f"  - slug: {cat['slug']}")
    lines += [
        "",
        "voice:",
        f"  tone: \"{data['voice']['tone']}\"",
        f"  persona: \"{data['voice']['persona']}\"",
        "",
        "article_types:",
        "  deep_dive:",
        f"    word_count: [2500, 3500]",
        f"    frequency: weekly",
        "  listicle:",
        f"    word_count: [1500, 2000]",
        f"    frequency: biweekly",
        "",
        "qa:",
        f"  min_score: 7.0",
        f"  publish_threshold: {data['qa']['publish_threshold']}",
        "",
        "meta:",
        f"  created_at: \"{data['meta']['created_at']}\"",
        f"  provisioned_by: provision.py",
    ]
    return "\n".join(lines) + "\n"

# ── Supabase row ───────────────────────────────────────────────────────────────

def build_supabase_row(args, preset: dict) -> dict:
    name = args.name or title_case(args.site)
    return {
        "domain":               args.domain,
        "name":                 name,
        "tier":                 args.tier,
        "site_key":             args.site,
        "template_id":          args.template,
        "publish_threshold":    preset["publish_threshold"],
        "auto_publish_enabled": False,
        "logo_url":             f"/sites/{args.site}/logo.svg",
        "favicon_url":          f"/sites/{args.site}/favicon.svg",
        "seo_defaults":         json.dumps({
            "title_suffix": f" | {name}",
            "description":  f"Evidence-based {args.niche} content.",
        }),
    }

# ── Commands ───────────────────────────────────────────────────────────────────

def cmd_new(args):
    section("🏗  Provisioning new site")
    print(f"  Site ID  : {args.site}")
    print(f"  Domain   : {args.domain}")
    print(f"  Niche    : {args.niche}")
    print(f"  Template : {args.template}")
    print(f"  Tier     : {args.tier}")
    if args.dry_run:
        print("\n  [DRY RUN — no files or DB rows will be created]\n")

    # ── Validate ───────────────────────────────────────────────────────────────
    validate_site_id(args.site)
    validate_domain(args.domain)

    if args.niche not in NICHE_PRESETS:
        available = ", ".join(NICHE_PRESETS.keys())
        die(f"Unknown niche '{args.niche}'. Available: {available}")

    if args.template not in VALID_TEMPLATES:
        die(f"Unknown template '{args.template}'. Available: {', '.join(VALID_TEMPLATES)}")

    if args.tier not in VALID_TIERS:
        die(f"Unknown tier '{args.tier}'. Available: {', '.join(VALID_TIERS)}")

    preset = NICHE_PRESETS[args.niche]

    # ── Check for existing site ────────────────────────────────────────────────
    yaml_path = SITES_DIR / f"{args.site}.yaml"
    if yaml_path.exists():
        die(f"Site config already exists: {yaml_path}\nDelete it first if you want to reprovision.")

    # ── Step 1: Write factory YAML ─────────────────────────────────────────────
    section("Step 1 of 4 — Factory site config")
    yaml_content = build_site_yaml(args, preset)

    if args.dry_run:
        print(f"\n  Would write: {yaml_path}\n")
        print(textwrap.indent(yaml_content, "    "))
    else:
        SITES_DIR.mkdir(parents=True, exist_ok=True)
        yaml_path.write_text(yaml_content, encoding="utf-8")
        ok(f"Written: {yaml_path}")

    # ── Step 2: Supabase row ───────────────────────────────────────────────────
    section("Step 2 of 4 — Supabase site record")
    row = build_supabase_row(args, preset)

    if args.dry_run:
        print("\n  Would INSERT into sites table:")
        for k, v in row.items():
            print(f"    {k}: {v}")
    else:
        sb = get_supabase()
        if sb:
            try:
                result = sb.table("sites").insert(row).execute()
                if result.data:
                    ok(f"Supabase row created (id: {result.data[0].get('id', '?')})")
                else:
                    warn("Supabase insert returned no data — check dashboard manually")
            except Exception as e:
                warn(f"Supabase insert failed: {e}")
                warn("You'll need to insert the row manually (SQL below)")
                _print_sql_fallback(row)
        else:
            warn("Supabase client unavailable (missing deps or env vars)")
            warn("Insert this row manually in Supabase SQL Editor:")
            _print_sql_fallback(row)

    # ── Step 2b: Factory Registry ─────────────────────────────────────────────
    section("Step 2b — Factory registry")

    if args.dry_run:
        print(f"\n  Would INSERT into factory_registry:")
        print(f"    site_key: {args.site}")
        print(f"    run_frequency: daily")
        print(f"    articles_per_run: 1")
        print(f"    status: active")
    else:
        try:
            from registry import Registry
            reg = Registry()
            if reg.register_site(args.site, run_frequency="daily", articles_per_run=1, status="active"):
                ok(f"Registered in factory_registry")
            else:
                warn("Failed to register in factory_registry")
        except Exception as e:
            warn(f"Registry registration failed: {e}")
            warn("Run manually: python registry.py register {args.site}")

    # ── Step 3: Assets scaffold ────────────────────────────────────────────────
    section("Step 3 of 4 — Site assets scaffold")
    assets_dir = FACTORY_ROOT.parent / "site-empire" / "public" / "sites" / args.site

    placeholder_svg = textwrap.dedent(f"""\
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 60">
          <rect width="200" height="60" fill="#f8f8f8"/>
          <text x="10" y="38" font-family="sans-serif" font-size="24" font-weight="bold" fill="#111">
            {args.name or title_case(args.site)}
          </text>
        </svg>
    """)

    favicon_svg = textwrap.dedent(f"""\
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">
          <rect width="32" height="32" rx="6" fill="#111"/>
          <text x="7" y="23" font-family="sans-serif" font-size="18" font-weight="bold" fill="#fff">
            {args.site[0].upper()}
          </text>
        </svg>
    """)

    files_to_create = {
        "logo.svg":      placeholder_svg,
        "logo-dark.svg": placeholder_svg.replace('fill="#111"', 'fill="#fff"').replace('fill="#f8f8f8"', 'fill="#222"'),
        "favicon.svg":   favicon_svg,
    }

    if args.dry_run:
        print(f"\n  Would create assets in: {assets_dir}/")
        for f in files_to_create:
            print(f"    {f}  (placeholder SVG)")
    else:
        assets_dir.mkdir(parents=True, exist_ok=True)
        for fname, content in files_to_create.items():
            fpath = assets_dir / fname
            if not fpath.exists():
                fpath.write_text(content, encoding="utf-8")
                ok(f"Created: {fpath.relative_to(FACTORY_ROOT.parent)}")
            else:
                info(f"Already exists (skipped): {fname}")

    # ── Step 4: Output instructions ────────────────────────────────────────────
    section("Step 4 of 4 — Remaining manual steps")
    print(textwrap.dedent(f"""
      These steps can't be automated and must be done manually:

      A) Vercel — add custom domain:
         vercel domains add {args.domain}
         (or: Vercel Dashboard → Project → Settings → Domains → Add)

      B) DNS — point your domain at Vercel:
         Add CNAME record:  {args.domain}  →  cname.vercel-dns.com

      C) Enable auto-publish when ready (in Supabase SQL Editor):
         UPDATE sites SET auto_publish_enabled = true WHERE site_key = '{args.site}';

      D) Replace placeholder SVGs with real logos:
         site-empire/public/sites/{args.site}/logo.svg
         site-empire/public/sites/{args.site}/logo-dark.svg
         site-empire/public/sites/{args.site}/favicon.svg

      E) If using a new template, create:
         site-empire/templates/{title_case(args.site)}/Home.tsx
         site-empire/templates/{title_case(args.site)}/index.tsx
         (skip if reusing '{args.template}')
    """))

    if not args.dry_run:
        section("✅  Provisioning complete")
        print(f"""
  Factory config : factory/config/sites/{args.site}.yaml
  Assets         : site-empire/public/sites/{args.site}/
  Supabase       : sites table (site_key='{args.site}')

  Run first article:
    cd factory
    python orchestrator.py run --site {args.site} --count 1
        """)


def cmd_list(args):
    section("📋  Provisioned sites")
    if not SITES_DIR.exists():
        die(f"Sites directory not found: {SITES_DIR}")

    yamls = sorted(SITES_DIR.glob("*.yaml"))
    if not yamls:
        print("  No sites found.")
        return

    print(f"  {'SITE ID':<25} {'DOMAIN':<35} {'NICHE':<25} {'TIER'}")
    print(f"  {'─'*25} {'─'*35} {'─'*25} {'─'*10}")

    for y in yamls:
        if HAS_YAML:
            try:
                data = yaml.safe_load(y.read_text(encoding="utf-8"))
                print(f"  {data.get('site_id','?'):<25} {data.get('domain','?'):<35} {data.get('niche','?'):<25} {data.get('tier','?')}")
            except Exception:
                print(f"  {y.stem:<25} (could not parse YAML)")
        else:
            print(f"  {y.stem}")

    print(f"\n  Total: {len(yamls)} site(s)")


def cmd_status(args):
    section(f"🔍  Site status: {args.site}")
    yaml_path = SITES_DIR / f"{args.site}.yaml"

    # Factory YAML
    if yaml_path.exists():
        ok(f"Factory config exists: {yaml_path.name}")
        if HAS_YAML:
            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
            print(f"       domain   : {data.get('domain')}")
            print(f"       niche    : {data.get('niche')}")
            print(f"       template : {data.get('template')}")
            print(f"       tier     : {data.get('tier')}")
    else:
        warn(f"Factory config MISSING: {yaml_path}")

    # Assets
    assets = FACTORY_ROOT.parent / "site-empire" / "public" / "sites" / args.site
    if assets.exists():
        files = list(assets.iterdir())
        ok(f"Assets folder exists ({len(files)} files)")
    else:
        warn(f"Assets folder missing: site-empire/public/sites/{args.site}/")

    # Supabase
    sb = get_supabase()
    if sb:
        try:
            result = sb.table("sites").select("*").eq("site_key", args.site).execute()
            if result.data:
                row = result.data[0]
                ok(f"Supabase row exists (id: {row.get('id')})")
                print(f"       auto_publish : {row.get('auto_publish_enabled')}")
                print(f"       threshold    : {row.get('publish_threshold')}")
            else:
                warn("Supabase row NOT FOUND")
        except Exception as e:
            warn(f"Supabase query failed: {e}")
    else:
        info("Supabase unavailable — skipping DB check")

    # Article count
    pipeline_dir = FACTORY_ROOT / "pipeline"
    for stage in ["topics", "research", "plans", "articles", "qa"]:
        stage_dir = pipeline_dir / stage
        if stage_dir.exists():
            count = len([f for f in stage_dir.glob("*.md") if args.site in f.name])
            if count:
                info(f"Pipeline/{stage}: {count} artifact(s) for this site")


def cmd_deactivate(args):
    section(f"🔴  Deactivating site: {args.site}")
    if args.dry_run:
        print("  [DRY RUN]")

    yaml_path = SITES_DIR / f"{args.site}.yaml"
    archive_dir = SITES_DIR / "_archived"

    if yaml_path.exists():
        if not args.dry_run:
            archive_dir.mkdir(exist_ok=True)
            archived = archive_dir / f"{args.site}.yaml"
            yaml_path.rename(archived)
            ok(f"Factory config archived to: config/sites/_archived/{args.site}.yaml")
        else:
            info(f"Would archive: {yaml_path}")
    else:
        warn(f"Factory config not found: {yaml_path}")

    sb = get_supabase()
    if sb:
        if not args.dry_run:
            try:
                sb.table("sites").update({"auto_publish_enabled": False}).eq("site_key", args.site).execute()
                ok("Supabase: auto_publish_enabled set to false")
            except Exception as e:
                warn(f"Supabase update failed: {e}")
        else:
            info(f"Would set auto_publish_enabled=false for site_key='{args.site}'")
    else:
        info("Supabase unavailable — update manually:")
        print(f"    UPDATE sites SET auto_publish_enabled = false WHERE site_key = '{args.site}';")

    print(f"\n  Site {args.site} deactivated. Factory will no longer process it.")
    print(f"  Supabase row and published articles are preserved.")
    print(f"  To re-activate: move YAML back from _archived/ and re-enable in Supabase.")


# ── SQL fallback ───────────────────────────────────────────────────────────────

def _print_sql_fallback(row: dict) -> None:
    cols = ", ".join(row.keys())
    vals = ", ".join(
        f"'{v}'" if isinstance(v, str) else
        ("true" if v is True else ("false" if v is False else str(v)))
        for v in row.values()
    )
    print(f"\n    INSERT INTO sites ({cols})")
    print(f"    VALUES ({vals});\n")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Provision new sites for Article Factory + Site Empire",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command")

    # ── new ────────────────────────────────────────────────────────────────────
    p_new = sub.add_parser("new", help="Provision a new site")
    p_new.add_argument("--site",     required=True,  help="Site ID slug")
    p_new.add_argument("--domain",   required=True,  help="Primary domain")
    p_new.add_argument("--niche",    required=True,  help=f"Content niche: {', '.join(NICHE_PRESETS)}")
    p_new.add_argument("--template", default="minimal-daily", help="Template slug")
    p_new.add_argument("--tier",     default="standard",      help="Tier: flagship|standard|micro")
    p_new.add_argument("--name",     default=None,            help="Display name (auto if omitted)")
    p_new.add_argument("--dry-run",  action="store_true",     help="Preview without writing")

    # ── list ───────────────────────────────────────────────────────────────────
    p_list = sub.add_parser("list", help="List all provisioned sites")

    # ── status ─────────────────────────────────────────────────────────────────
    p_status = sub.add_parser("status", help="Check provisioning status for a site")
    p_status.add_argument("--site", required=True)

    # ── deactivate ─────────────────────────────────────────────────────────────
    p_deac = sub.add_parser("deactivate", help="Deactivate a site (archive config, disable publish)")
    p_deac.add_argument("--site",    required=True)
    p_deac.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    dispatch = {
        "new":        cmd_new,
        "list":       cmd_list,
        "status":     cmd_status,
        "deactivate": cmd_deactivate,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
