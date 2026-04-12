#!/usr/bin/env python3
"""
provision.py - Site provisioning CLI for Article Factory + Site Empire

Usage:
    python provision.py new --site mysite --domain mysite.com --niche health-longevity
    python provision.py new --site mysite --domain mysite.com --niche finance --template magazine
    python provision.py list
    python provision.py status --site mysite
    python provision.py verify --site mysite
    python provision.py deactivate --site mysite

Options:
    --site          Site ID / slug (lowercase, hyphens ok, e.g. "lamp-hill")
    --domain        Primary domain (e.g. "lamphill.org")
    --niche         Content niche (e.g. "health-longevity", "finance", "fitness")
    --template      Template slug (default: "magazine")
    --tier          Site tier: flagship | standard | micro (default: standard)
    --name          Display name (default: title-cased from --site)
    --dry-run       Show what would happen without doing it
"""

import argparse
import os
import sys
import json
import textwrap
from datetime import datetime, timezone
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
except ImportError:
    pass

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from supabase import create_client
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False

# ── Paths ──────────────────────────────────────────────────────────────────────
FACTORY_ROOT = Path(__file__).parent
SITES_DIR = FACTORY_ROOT / "config" / "sites"
SITE_EMPIRE_ROOT = FACTORY_ROOT.parent / "site-empire"

# ── Niche → category presets ───────────────────────────────────────────────────
NICHE_PRESETS = {
    "health-longevity": {
        "categories": [
            {"slug": "supplements", "label": "Supplements", "description": "Evidence-based supplement guides, dosing, and mechanisms"},
            {"slug": "fitness", "label": "Fitness", "description": "Exercise science, training protocols, and recovery"},
            {"slug": "nutrition", "label": "Nutrition", "description": "Diet strategies, macronutrients, and metabolic health"},
            {"slug": "sleep", "label": "Sleep", "description": "Sleep optimization, circadian health, and recovery"},
            {"slug": "labs", "label": "Labs", "description": "Blood work, biomarkers, testing protocols, and interpretation"},
            {"slug": "endocrine", "label": "Endocrine", "description": "Hormones, thyroid, testosterone, and metabolic regulation"},
            {"slug": "hot-cold", "label": "Hot & Cold", "description": "Sauna, cold plunge, heat/cold therapy protocols"},
            {"slug": "leading-voices", "label": "Leading Voices", "description": "Profiles and insights from longevity researchers"},
            {"slug": "events", "label": "Events", "description": "Conferences, summits, and community gatherings"},
        ],
        "voice": {
            "tone": "authoritative yet approachable",
            "persona": "physician-researcher translating clinical evidence into actionable strategies",
        },
        "publish_threshold": 8.0,
    },
    "finance": {
        "categories": [
            {"slug": "investing", "label": "Investing", "description": "Long-term investment strategies and portfolio management"},
            {"slug": "trading", "label": "Trading", "description": "Active trading strategies and market analysis"},
            {"slug": "tax", "label": "Tax", "description": "Tax optimization, planning, and compliance"},
            {"slug": "real-estate", "label": "Real Estate", "description": "Property investment and real estate strategies"},
            {"slug": "crypto", "label": "Crypto", "description": "Cryptocurrency and blockchain investments"},
            {"slug": "retirement", "label": "Retirement", "description": "Retirement planning and wealth preservation"},
            {"slug": "news", "label": "News", "description": "Market news and economic analysis"},
        ],
        "voice": {
            "tone": "clear, direct, no hype",
            "persona": "independent analyst cutting through noise",
        },
        "publish_threshold": 8.0,
    },
    "fitness": {
        "categories": [
            {"slug": "training", "label": "Training", "description": "Workout programs and exercise technique"},
            {"slug": "nutrition", "label": "Nutrition", "description": "Diet, macros, and meal planning"},
            {"slug": "recovery", "label": "Recovery", "description": "Rest, sleep, and injury prevention"},
            {"slug": "gear", "label": "Gear", "description": "Equipment reviews and recommendations"},
            {"slug": "mindset", "label": "Mindset", "description": "Mental toughness and motivation"},
            {"slug": "programs", "label": "Programs", "description": "Complete training programs and challenges"},
        ],
        "voice": {
            "tone": "motivating but evidence-based",
            "persona": "experienced coach who reads the research",
        },
        "publish_threshold": 7.5,
    },
    "technology": {
        "categories": [
            {"slug": "ai", "label": "AI", "description": "Artificial intelligence and machine learning"},
            {"slug": "software", "label": "Software", "description": "Software development and engineering"},
            {"slug": "hardware", "label": "Hardware", "description": "Computer hardware and devices"},
            {"slug": "startups", "label": "Startups", "description": "Startup ecosystem and entrepreneurship"},
            {"slug": "security", "label": "Security", "description": "Cybersecurity and privacy"},
            {"slug": "tutorials", "label": "Tutorials", "description": "How-to guides and technical tutorials"},
        ],
        "voice": {
            "tone": "technically precise, jargon where earned",
            "persona": "senior engineer who explains things well",
        },
        "publish_threshold": 8.0,
    },
    "personal-development": {
        "categories": [
            {"slug": "habits", "label": "Habits", "description": "Building and maintaining good habits"},
            {"slug": "productivity", "label": "Productivity", "description": "Getting more done with less effort"},
            {"slug": "mindset", "label": "Mindset", "description": "Mental models and thinking frameworks"},
            {"slug": "relationships", "label": "Relationships", "description": "Interpersonal skills and connections"},
            {"slug": "career", "label": "Career", "description": "Professional growth and career development"},
            {"slug": "learning", "label": "Learning", "description": "Learning techniques and skill acquisition"},
        ],
        "voice": {
            "tone": "honest, direct, no toxic positivity",
            "persona": "thoughtful practitioner sharing what actually works",
        },
        "publish_threshold": 7.5,
    },
    "trading": {
        "categories": [
            {"slug": "the-strat", "label": "The Strat", "description": "Rob Smith's Strat methodology and broadening formations"},
            {"slug": "setups", "label": "Setups", "description": "Specific trade setups with entry, stop, and targets"},
            {"slug": "futures", "label": "Futures", "description": "Futures trading including ES, NQ, CL"},
            {"slug": "stocks", "label": "Stocks", "description": "Stock trading setups and equity analysis"},
            {"slug": "options", "label": "Options", "description": "Options strategies, Greeks, and flow analysis"},
            {"slug": "risk-management", "label": "Risk Management", "description": "Position sizing, stops, and capital preservation"},
            {"slug": "psychology", "label": "Psychology", "description": "Trading mindset, discipline, and emotional control"},
            {"slug": "market-structure", "label": "Market Structure", "description": "Volume profile, order flow, and auction theory"},
            {"slug": "tools", "label": "Tools", "description": "Trading software, charting, and platforms"},
            {"slug": "education", "label": "Education", "description": "Foundational concepts and learning resources"},
        ],
        "voice": {
            "tone": "direct, no hype, practitioner-first",
            "persona": "active trader who has logged screen time and shows the work",
        },
        "publish_threshold": 8.0,
    },
}

# V2 templates + legacy
VALID_TEMPLATES = ["magazine", "minimal", "editorial", "docs", "landing", "minimal-daily", "lamphill"]
VALID_TIERS = ["flagship", "standard", "micro"]

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
    print(f"\n[X] {msg}\n", file=sys.stderr)
    sys.exit(1)

def ok(msg: str) -> None:
    print(f"  [OK] {msg}")

def warn(msg: str) -> None:
    print(f"  [WARN] {msg}")

def info(msg: str) -> None:
    print(f"  [INFO] {msg}")

def fail(msg: str) -> None:
    print(f"  [FAIL] {msg}")

def section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")

# ── Supabase helpers ───────────────────────────────────────────────────────────

def get_supabase_config():
    """Get Supabase URL and key from environment."""
    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL") or os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_SERVICE_KEY")
    return url, key

def get_supabase_client():
    """Get Supabase client if available."""
    url, key = get_supabase_config()
    if not url or not key:
        return None
    if not HAS_SUPABASE:
        return None
    return create_client(url, key)

def supabase_query(table: str, method: str = "GET", params: dict = None, data: dict = None):
    """
    Execute Supabase REST API query directly via HTTP.
    Fallback when supabase-py is not available.
    """
    url, key = get_supabase_config()
    if not url or not key:
        return None, "Supabase credentials not configured"

    if not HAS_REQUESTS:
        return None, "requests library not available"

    endpoint = f"{url}/rest/v1/{table}"
    if params:
        query_parts = []
        for k, v in params.items():
            query_parts.append(f"{k}={v}")
        endpoint += "?" + "&".join(query_parts)

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    try:
        if method == "GET":
            resp = requests.get(endpoint, headers=headers, timeout=10)
        elif method == "POST":
            resp = requests.post(endpoint, headers=headers, json=data, timeout=10)
        elif method == "PATCH":
            resp = requests.patch(endpoint, headers=headers, json=data, timeout=10)
        else:
            return None, f"Unknown method: {method}"

        if resp.status_code >= 400:
            return None, f"HTTP {resp.status_code}: {resp.text[:200]}"

        return resp.json(), None
    except Exception as e:
        return None, str(e)

def check_site_in_supabase(site_key: str):
    """Check if site exists in Supabase sites table."""
    # Try supabase-py first
    sb = get_supabase_client()
    if sb:
        try:
            result = sb.table("sites").select("*").eq("site_key", site_key).execute()
            return result.data[0] if result.data else None
        except:
            pass

    # Fallback to HTTP
    data, err = supabase_query("sites", "GET", {"site_key": f"eq.{site_key}", "select": "*"})
    if data and len(data) > 0:
        return data[0]
    return None

def insert_site_to_supabase(row: dict):
    """Insert site into Supabase sites table."""
    # Try supabase-py first
    sb = get_supabase_client()
    if sb:
        try:
            result = sb.table("sites").insert(row).execute()
            if result.data:
                return result.data[0], None
        except Exception as e:
            pass

    # Fallback to HTTP
    data, err = supabase_query("sites", "POST", data=row)
    if data and len(data) > 0:
        return data[0], None
    return None, err

def check_registry(site_key: str):
    """Check if site exists in factory_registry."""
    data, err = supabase_query("factory_registry", "GET", {"site_key": f"eq.{site_key}", "select": "*"})
    if data and len(data) > 0:
        return data[0]
    return None

def insert_registry(site_key: str, frequency: str = "daily", articles: int = 1):
    """Insert site into factory_registry."""
    row = {
        "site_key": site_key,
        "run_frequency": frequency,
        "articles_per_run": articles,
        "status": "active",
    }
    data, err = supabase_query("factory_registry", "POST", data=row)
    if data:
        return True, None
    return False, err

# ── YAML generation ────────────────────────────────────────────────────────────

def build_site_yaml(args, preset: dict) -> str:
    """Generate full site YAML configuration."""
    name = args.name or title_case(args.site)

    data = {
        "site_id": args.site,
        "site_name": name,
        "domain": args.domain,
        "tier": args.tier,
        "niche": args.niche,
        "template": args.template,

        "categories": preset["categories"],

        "voice": preset["voice"],

        "article_types": [
            {
                "type_id": "deep_dive",
                "label": "Deep Dive",
                "description": "Long-form, research-heavy article",
                "word_count_min": 2000,
                "word_count_max": 3500,
                "frequency": "weekly",
                "enabled": True,
            },
            {
                "type_id": "listicle",
                "label": "Listicle",
                "description": "Scannable list-format article",
                "word_count_min": 1200,
                "word_count_max": 2000,
                "frequency": "2x_week",
                "enabled": True,
            },
        ],

        "quality": {
            "publish_threshold": preset["publish_threshold"],
            "rewrite_threshold": 6.0,
            "max_rewrites": 2,
        },

        "output": {
            "frontmatter_template": {
                "site": args.site,
                "status": "draft",
                "author": name,
            },
            "filename_pattern": "YYYY-MM-DD-{slug}.md",
        },

        "meta": {
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "provisioned_by": "provision.py",
        },
    }

    if HAS_YAML:
        return yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=False)

    # Fallback: hand-roll YAML
    lines = [
        f"site_id: {data['site_id']}",
        f"site_name: \"{data['site_name']}\"",
        f"domain: {data['domain']}",
        f"tier: {data['tier']}",
        f"niche: {data['niche']}",
        f"template: {data['template']}",
        "",
        "categories:",
    ]
    for cat in data["categories"]:
        lines.append(f"  - slug: {cat['slug']}")
        lines.append(f"    label: \"{cat['label']}\"")
        lines.append(f"    description: \"{cat['description']}\"")
    lines += [
        "",
        "voice:",
        f"  tone: \"{data['voice']['tone']}\"",
        f"  persona: \"{data['voice']['persona']}\"",
        "",
        "quality:",
        f"  publish_threshold: {data['quality']['publish_threshold']}",
        f"  rewrite_threshold: {data['quality']['rewrite_threshold']}",
        f"  max_rewrites: {data['quality']['max_rewrites']}",
    ]
    return "\n".join(lines) + "\n"

def build_supabase_row(args, preset: dict) -> dict:
    """Build row for Supabase sites table."""
    name = args.name or title_case(args.site)
    return {
        "domain": args.domain,
        "name": name,
        "tier": args.tier,
        "site_key": args.site,
        "template_id": args.template,
        "publish_threshold": preset["publish_threshold"],
        "auto_publish_enabled": False,
        "logo_url": f"/sites/{args.site}/logo.svg",
        "favicon_url": f"/sites/{args.site}/favicon.svg",
        "seo_defaults": {
            "title_pattern": "{post_title} | {site_name}",
            "meta_description_pattern": "{excerpt}",
        },
    }

# ── Commands ───────────────────────────────────────────────────────────────────

def cmd_new(args):
    section("Provisioning new site")
    print(f"  Site ID  : {args.site}")
    print(f"  Domain   : {args.domain}")
    print(f"  Niche    : {args.niche}")
    print(f"  Template : {args.template}")
    print(f"  Tier     : {args.tier}")
    if args.dry_run:
        print("\n  [DRY RUN - no files or DB rows will be created]\n")

    # Validate
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
    results = {"yaml": False, "supabase": False, "registry": False, "assets": False}

    # Check for existing site
    yaml_path = SITES_DIR / f"{args.site}.yaml"
    if yaml_path.exists():
        die(f"Site config already exists: {yaml_path}\nDelete it first or use 'status' command.")

    # Step 1: Write factory YAML
    section("Step 1/4 - Factory site config")
    yaml_content = build_site_yaml(args, preset)
    cat_count = len(preset["categories"])

    if args.dry_run:
        print(f"\n  Would write: {yaml_path}")
        print(f"  Categories: {cat_count}")
        print(f"\n  Preview:\n")
        for line in yaml_content.split("\n")[:30]:
            print(f"    {line}")
        print("    ...")
    else:
        SITES_DIR.mkdir(parents=True, exist_ok=True)
        yaml_path.write_text(yaml_content, encoding="utf-8")
        ok(f"Written: {yaml_path.name} ({cat_count} categories)")
        results["yaml"] = True

    # Step 2: Supabase site record
    section("Step 2/4 - Supabase site record")
    row = build_supabase_row(args, preset)

    if args.dry_run:
        print("\n  Would INSERT into sites table:")
        for k, v in row.items():
            print(f"    {k}: {v}")
    else:
        # Check if already exists
        existing = check_site_in_supabase(args.site)
        if existing:
            ok(f"Site already exists in Supabase (id: {existing.get('id', '?')})")
            results["supabase"] = True
        else:
            data, err = insert_site_to_supabase(row)
            if data:
                ok(f"Supabase row created (id: {data.get('id', '?')})")
                results["supabase"] = True
            else:
                warn(f"Supabase insert failed: {err}")
                warn("Insert manually with SQL:")
                _print_sql_fallback("sites", row)

    # Step 3: Factory registry
    section("Step 3/4 - Factory registry")

    if args.dry_run:
        print(f"\n  Would INSERT into factory_registry:")
        print(f"    site_key: {args.site}")
        print(f"    run_frequency: daily")
        print(f"    articles_per_run: 1")
    else:
        existing = check_registry(args.site)
        if existing:
            ok(f"Already registered (status: {existing.get('status', '?')})")
            results["registry"] = True
        else:
            success, err = insert_registry(args.site)
            if success:
                ok("Registered in factory_registry")
                results["registry"] = True
            else:
                warn(f"Registry insert failed: {err}")
                warn("Run manually: python registry.py register " + args.site)

    # Step 4: Assets scaffold
    section("Step 4/4 - Site assets")
    assets_dir = SITE_EMPIRE_ROOT / "public" / "sites" / args.site
    name = args.name or title_case(args.site)

    placeholder_svg = f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 60">
  <rect width="200" height="60" fill="#f8f8f8"/>
  <text x="10" y="38" font-family="sans-serif" font-size="24" font-weight="bold" fill="#111">{name}</text>
</svg>'''

    placeholder_dark_svg = f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 60">
  <rect width="200" height="60" fill="#222"/>
  <text x="10" y="38" font-family="sans-serif" font-size="24" font-weight="bold" fill="#fff">{name}</text>
</svg>'''

    favicon_svg = f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">
  <rect width="32" height="32" rx="6" fill="#111"/>
  <text x="7" y="23" font-family="sans-serif" font-size="18" font-weight="bold" fill="#fff">{args.site[0].upper()}</text>
</svg>'''

    files = {
        "logo.svg": placeholder_svg,
        "logo-dark.svg": placeholder_dark_svg,
        "favicon.svg": favicon_svg,
    }

    if args.dry_run:
        print(f"\n  Would create: {assets_dir}/")
        for f in files:
            print(f"    {f}")
    else:
        if SITE_EMPIRE_ROOT.exists():
            assets_dir.mkdir(parents=True, exist_ok=True)
            created = 0
            for fname, content in files.items():
                fpath = assets_dir / fname
                if not fpath.exists():
                    fpath.write_text(content, encoding="utf-8")
                    created += 1
            if created > 0:
                ok(f"Created {created} asset file(s) in site-empire/public/sites/{args.site}/")
            else:
                info("Assets already exist")
            results["assets"] = True
        else:
            warn(f"site-empire not found at {SITE_EMPIRE_ROOT}")
            warn("Create assets manually in site-empire/public/sites/" + args.site)

    # Summary
    if not args.dry_run:
        section("Provisioning Summary")

        all_ok = all(results.values())
        for step, success in results.items():
            status = "[OK]" if success else "[FAIL]"
            print(f"  {status} {step}")

        if all_ok:
            print(f"\n  Site '{args.site}' fully provisioned!")
        else:
            print(f"\n  Site '{args.site}' partially provisioned. Fix failed steps above.")

        section("Manual Steps Required")
        print(f"""
  1. Vercel - add custom domain:
     vercel domains add {args.domain}

  2. DNS - point domain to Vercel:
     CNAME {args.domain} -> cname.vercel-dns.com

  3. Replace placeholder logos:
     site-empire/public/sites/{args.site}/logo.svg
     site-empire/public/sites/{args.site}/logo-dark.svg

  4. Enable auto-publish (when ready):
     UPDATE sites SET auto_publish_enabled = true WHERE site_key = '{args.site}';

  5. Run first article:
     python orchestrator.py run --site {args.site} --count 1
""")


def cmd_list(args):
    section("Provisioned Sites")
    if not SITES_DIR.exists():
        die(f"Sites directory not found: {SITES_DIR}")

    yamls = sorted(SITES_DIR.glob("*.yaml"))
    if not yamls:
        print("  No sites found.")
        return

    print(f"  {'SITE ID':<20} {'DOMAIN':<25} {'NICHE':<20} {'CATS':<5} {'TIER'}")
    print(f"  {'─'*20} {'─'*25} {'─'*20} {'─'*5} {'─'*10}")

    for y in yamls:
        if y.name.startswith("_"):
            continue
        if HAS_YAML:
            try:
                data = yaml.safe_load(y.read_text(encoding="utf-8"))
                site_id = data.get('site_id', y.stem)
                domain = data.get('domain', '?')
                niche = data.get('niche', '?')
                cats = len(data.get('categories', []))
                tier = data.get('tier', '?')
                print(f"  {site_id:<20} {domain:<25} {niche:<20} {cats:<5} {tier}")
            except Exception:
                print(f"  {y.stem:<20} (parse error)")
        else:
            print(f"  {y.stem}")

    print(f"\n  Total: {len(yamls)} site(s)")


def cmd_status(args):
    section(f"Site Status: {args.site}")

    all_ok = True

    # Factory YAML
    yaml_path = SITES_DIR / f"{args.site}.yaml"
    if yaml_path.exists():
        ok(f"Factory config: {yaml_path.name}")
        if HAS_YAML:
            data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
            print(f"       domain     : {data.get('domain')}")
            print(f"       niche      : {data.get('niche')}")
            print(f"       template   : {data.get('template')}")
            print(f"       categories : {len(data.get('categories', []))}")
    else:
        fail(f"Factory config MISSING: {yaml_path}")
        all_ok = False

    # Supabase site
    site_data = check_site_in_supabase(args.site)
    if site_data:
        ok(f"Supabase sites: id={site_data.get('id', '?')[:8]}...")
        print(f"       domain      : {site_data.get('domain')}")
        print(f"       auto_publish: {site_data.get('auto_publish_enabled')}")
        print(f"       threshold   : {site_data.get('publish_threshold')}")
    else:
        fail("Supabase sites: NOT FOUND")
        all_ok = False

    # Factory registry
    reg_data = check_registry(args.site)
    if reg_data:
        ok(f"Factory registry: status={reg_data.get('status')}")
        print(f"       frequency   : {reg_data.get('run_frequency')}")
        print(f"       articles    : {reg_data.get('articles_per_run')}")
        print(f"       next_run    : {reg_data.get('next_run_at', 'not set')}")
    else:
        fail("Factory registry: NOT FOUND")
        all_ok = False

    # Assets
    assets_dir = SITE_EMPIRE_ROOT / "public" / "sites" / args.site
    if assets_dir.exists():
        files = list(assets_dir.glob("*.svg"))
        ok(f"Assets: {len(files)} file(s)")
    else:
        fail(f"Assets: directory not found")
        all_ok = False

    print()
    if all_ok:
        print("  All components OK")
    else:
        print("  Some components missing - run provision.py new or fix manually")


def cmd_verify(args):
    """Verify all components are properly set up for a site."""
    cmd_status(args)  # Same as status for now


def cmd_deactivate(args):
    section(f"Deactivating site: {args.site}")
    if args.dry_run:
        print("  [DRY RUN]")

    yaml_path = SITES_DIR / f"{args.site}.yaml"
    archive_dir = SITES_DIR / "_archived"

    # Archive YAML
    if yaml_path.exists():
        if not args.dry_run:
            archive_dir.mkdir(exist_ok=True)
            archived = archive_dir / f"{args.site}.yaml"
            yaml_path.rename(archived)
            ok(f"Archived config to: _archived/{args.site}.yaml")
        else:
            info(f"Would archive: {yaml_path}")
    else:
        warn(f"Factory config not found: {yaml_path}")

    # Disable in Supabase
    url, key = get_supabase_config()
    if url and key and HAS_REQUESTS:
        if not args.dry_run:
            data, err = supabase_query(
                "sites",
                "PATCH",
                {"site_key": f"eq.{args.site}"},
                {"auto_publish_enabled": False}
            )
            if not err:
                ok("Disabled auto_publish in Supabase")
            else:
                warn(f"Supabase update failed: {err}")
        else:
            info("Would disable auto_publish in Supabase")
    else:
        info("Update Supabase manually:")
        print(f"    UPDATE sites SET auto_publish_enabled = false WHERE site_key = '{args.site}';")

    # Update registry status
    if not args.dry_run:
        data, err = supabase_query(
            "factory_registry",
            "PATCH",
            {"site_key": f"eq.{args.site}"},
            {"status": "paused"}
        )
        if not err:
            ok("Set registry status to 'paused'")
        else:
            warn(f"Registry update failed: {err}")

    print(f"\n  Site '{args.site}' deactivated.")
    print("  To reactivate: move YAML back and update status in Supabase.")


def _print_sql_fallback(table: str, row: dict) -> None:
    """Print SQL INSERT statement as fallback."""
    cols = ", ".join(row.keys())
    vals = []
    for v in row.values():
        if v is None:
            vals.append("NULL")
        elif isinstance(v, bool):
            vals.append("true" if v else "false")
        elif isinstance(v, (int, float)):
            vals.append(str(v))
        elif isinstance(v, dict):
            vals.append(f"'{json.dumps(v)}'")
        else:
            vals.append(f"'{v}'")

    print(f"\n    INSERT INTO {table} ({cols})")
    print(f"    VALUES ({', '.join(vals)});\n")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Provision new sites for Article Factory + Site Empire",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command")

    # new
    p_new = sub.add_parser("new", help="Provision a new site")
    p_new.add_argument("--site", required=True, help="Site ID slug")
    p_new.add_argument("--domain", required=True, help="Primary domain")
    p_new.add_argument("--niche", required=True, help=f"Niche: {', '.join(NICHE_PRESETS)}")
    p_new.add_argument("--template", default="magazine", help="Template (default: magazine)")
    p_new.add_argument("--tier", default="standard", help="Tier: flagship|standard|micro")
    p_new.add_argument("--name", default=None, help="Display name")
    p_new.add_argument("--dry-run", action="store_true", help="Preview only")

    # list
    sub.add_parser("list", help="List all provisioned sites")

    # status
    p_status = sub.add_parser("status", help="Check site provisioning status")
    p_status.add_argument("--site", required=True)

    # verify (alias for status)
    p_verify = sub.add_parser("verify", help="Verify site is fully provisioned")
    p_verify.add_argument("--site", required=True)

    # deactivate
    p_deac = sub.add_parser("deactivate", help="Deactivate a site")
    p_deac.add_argument("--site", required=True)
    p_deac.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    dispatch = {
        "new": cmd_new,
        "list": cmd_list,
        "status": cmd_status,
        "verify": cmd_verify,
        "deactivate": cmd_deactivate,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
