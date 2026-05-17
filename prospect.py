#!/usr/bin/env python3
"""
prospect.py - Autonomous niche discovery for Article Factory

Finds content opportunities you didn't know to look for. Pulls growing
subreddits, identifies content gaps, clusters into niches, scores each
opportunity, and outputs a ranked list ready to pipe into scout.py.

Usage:
    python prospect.py run                        # Full discovery run
    python prospect.py run --filter health        # Focus on a vertical
    python prospect.py run --min-score 60         # Only show high scorers
    python prospect.py run --limit 20             # Top 20 opportunities
    python prospect.py trends                     # What's accelerating right now
    python prospect.py gaps --subreddit fitness   # Find gaps in a specific community
    python prospect.py pipeline                   # Auto-pipe top picks to scout.py

Verticals (built-in filters):
    health | finance | fitness | technology | lifestyle | professional
"""

import argparse
import json
import os
import re
import sys
import time
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus, urlencode
import urllib.request
import urllib.error

# ── Optional deps ──────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

# ── Paths ──────────────────────────────────────────────────────────────────────
FACTORY_ROOT   = Path(__file__).parent
PROSPECTS_DIR  = FACTORY_ROOT / "pipeline" / "prospects"
SCOUTS_DIR     = FACTORY_ROOT / "pipeline" / "scouts"

# ── Reddit headers ─────────────────────────────────────────────────────────────
REDDIT_HEADERS = {
    "User-Agent": "ArticleFactoryProspector/1.0 (niche discovery; contact via github)"
}

# ── Vertical definitions ───────────────────────────────────────────────────────
# Seed subreddits per vertical — used to find adjacent communities
VERTICALS = {
    "health": {
        "seeds": ["longevity", "health", "nutrition", "supplements", "sleep",
                  "intermittentfasting", "ketoscience", "peptides", "nootropics",
                  "herbalism", "ayurveda", "biohacking"],
        "monetization_multiplier": 1.4,  # High RPM niche
        "your_advantage": "physician credibility — hard for others to replicate",
    },
    "finance": {
        "seeds": ["personalfinance", "investing", "financialindependence",
                  "dividends", "ValueInvesting", "options", "algotrading",
                  "CryptoCurrency", "realestateinvesting", "fatFIRE"],
        "monetization_multiplier": 1.5,
        "your_advantage": "active trader with real P&L — credibility others fake",
    },
    "fitness": {
        "seeds": ["fitness", "weightlifting", "running", "cycling", "yoga",
                  "crossfit", "bodyweightfitness", "powerlifting", "martialarts",
                  "swimming", "hiking"],
        "monetization_multiplier": 1.1,
        "your_advantage": "physician lens on performance science",
    },
    "technology": {
        "seeds": ["technology", "artificial", "MachineLearning", "programming",
                  "homelab", "selfhosted", "cybersecurity", "datascience",
                  "webdev", "sysadmin"],
        "monetization_multiplier": 1.2,
        "your_advantage": "digital pathology / lab informatics insider",
    },
    "lifestyle": {
        "seeds": ["minimalism", "zerowaste", "vandwellers", "solotravel",
                  "digitalnomad", "productivity", "getdisciplined", "selfimprovement",
                  "stoicism", "meditation"],
        "monetization_multiplier": 0.9,
        "your_advantage": "philosophical depth (Notre Dame background)",
    },
    "professional": {
        "seeds": ["medicine", "nursing", "labrats", "pathology", "pharmacy",
                  "consulting", "freelance", "Entrepreneur", "smallbusiness",
                  "sidehustle"],
        "monetization_multiplier": 1.3,
        "your_advantage": "physician + consultant + entrepreneur — rare combination",
    },
}

# Subreddits to always exclude (too broad, too competitive, not content-site friendly)
EXCLUDE_SUBS = {
    "AskReddit", "funny", "gaming", "pics", "videos", "worldnews", "news",
    "movies", "Music", "television", "sports", "nfl", "nba", "soccer",
    "politics", "conspiracy", "teenagers", "relationship_advice", "AmItheAsshole",
    "tifu", "mildlyinteresting", "todayilearned", "LifeProTips", "showerthoughts",
    "unpopularopinion", "changemyview", "memes", "dankmemes", "Jokes",
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def slugify(text: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', text.lower().strip()).strip('-')

def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")

def info(msg: str) -> None:
    print(f"  ℹ  {msg}")

def ok(msg: str) -> None:
    print(f"  ✓  {msg}")

def warn(msg: str) -> None:
    print(f"  ⚠  {msg}", file=sys.stderr)

def fetch_json(url: str, headers: dict = None, retries: int = 3) -> dict | None:
    req = urllib.request.Request(url, headers=headers or {})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** (attempt + 1)
                info(f"Rate limited — waiting {wait}s...")
                time.sleep(wait)
            elif e.code in (403, 404):
                return None
            else:
                warn(f"HTTP {e.code}: {url}")
                return None
        except Exception as e:
            if attempt == retries - 1:
                warn(f"Failed: {url} — {e}")
            time.sleep(1)
    return None

# ── Reddit Discovery ───────────────────────────────────────────────────────────

def get_subreddit_info(subreddit: str) -> dict | None:
    """Get metadata for a specific subreddit."""
    data = fetch_json(
        f"https://www.reddit.com/r/{subreddit}/about.json",
        REDDIT_HEADERS
    )
    if not data or data.get("kind") != "t5":
        return None

    sr = data.get("data", {})
    return {
        "name":           sr.get("display_name", ""),
        "title":          sr.get("title", ""),
        "subscribers":    sr.get("subscribers", 0),
        "active_users":   sr.get("active_user_count", 0),
        "description":    (sr.get("public_description") or "")[:400],
        "created_utc":    sr.get("created_utc", 0),
        "over18":         sr.get("over18", False),
        "url":            f"https://reddit.com/r/{sr.get('display_name', '')}",
    }


def get_related_subreddits(subreddit: str, limit: int = 10) -> list[str]:
    """Find subreddits mentioned or linked from a subreddit's sidebar/wiki."""
    names = set()

    # Search Reddit for similar communities
    url = f"https://www.reddit.com/search.json?q={quote_plus(subreddit)}&type=sr&limit={limit}"
    data = fetch_json(url, REDDIT_HEADERS)
    if data:
        for child in data.get("data", {}).get("children", []):
            name = child.get("data", {}).get("display_name", "")
            if name and name not in EXCLUDE_SUBS:
                names.add(name)

    time.sleep(0.5)
    return list(names)[:limit]


def analyze_subreddit_content_gap(subreddit: str) -> dict:
    """
    Analyze a subreddit for content gap signals:
    - High engagement posts = people care about this
    - Questions with many comments but no great answers = content gap
    - Recurring themes = what to write about
    """
    gap_signals = {
        "top_questions":     [],
        "recurring_themes":  [],
        "engagement_score":  0,
        "gap_score":         0,
    }

    # Get top posts
    top_url = f"https://www.reddit.com/r/{subreddit}/top.json?t=month&limit=25"
    top_data = fetch_json(top_url, REDDIT_HEADERS)

    if not top_data:
        return gap_signals

    posts = [c.get("data", {}) for c in top_data.get("data", {}).get("children", [])]
    posts = [p for p in posts if not p.get("stickied")]

    if not posts:
        return gap_signals

    # Engagement score = average (score + comments*3) of top posts
    engagements = [p.get("score", 0) + p.get("num_comments", 0) * 3 for p in posts]
    gap_signals["engagement_score"] = int(sum(engagements) / len(engagements)) if engagements else 0

    # Find question posts
    for p in posts:
        title = p.get("title", "")
        if any(w in title.lower() for w in ["?", "how", "what", "why", "best", "recommend", "help"]):
            gap_signals["top_questions"].append({
                "title":    title,
                "score":    p.get("score", 0),
                "comments": p.get("num_comments", 0),
            })

    # High comment-to-score ratio = unanswered demand
    # People asking but no one satisfying them definitively
    high_comment_ratio = [
        p for p in posts
        if p.get("num_comments", 0) > 50
        and p.get("score", 0) > 0
        and (p.get("num_comments", 0) / max(p.get("score", 1), 1)) > 0.3
    ]

    gap_signals["gap_score"] = min(100, len(high_comment_ratio) * 15 + len(gap_signals["top_questions"]) * 5)
    gap_signals["top_questions"] = sorted(
        gap_signals["top_questions"],
        key=lambda x: x["comments"],
        reverse=True
    )[:5]

    time.sleep(0.5)
    return gap_signals


def discover_subreddits_from_seeds(seeds: list[str], limit_per_seed: int = 5) -> list[dict]:
    """
    Expand seed subreddits to find adjacent communities.
    Returns enriched subreddit data.
    """
    discovered = {}

    for seed in seeds:
        if seed in EXCLUDE_SUBS:
            continue

        # Get the seed itself
        info_data = get_subreddit_info(seed)
        if info_data and not info_data.get("over18"):
            discovered[seed] = info_data

        # Find related
        related = get_related_subreddits(seed, limit_per_seed)
        for name in related:
            if name in discovered or name in EXCLUDE_SUBS:
                continue
            sr_data = get_subreddit_info(name)
            if sr_data and not sr_data.get("over18"):
                discovered[name] = sr_data
            time.sleep(0.3)

        time.sleep(0.5)

    return list(discovered.values())


def filter_promising_subreddits(subreddits: list[dict]) -> list[dict]:
    """
    Filter for subreddits that show content site potential:
    - Not too big (too competitive) or too small (no audience)
    - Active community
    - Not a meme/entertainment sub
    """
    promising = []

    for sr in subreddits:
        subs = sr.get("subscribers", 0)
        active = sr.get("active_users", 0)

        # Size filter: 10k - 2M subscribers sweet spot
        if subs < 10_000 or subs > 2_000_000:
            continue

        # Activity ratio: at least 0.1% active at any time
        if subs > 0 and active / subs < 0.001:
            continue

        # Exclude known entertainment/meme patterns
        name  = sr.get("name", "").lower()
        title = sr.get("title", "").lower()
        desc  = sr.get("description", "").lower()

        skip_keywords = ["meme", "shitpost", "circlejerk", "humor", "funny",
                         "copypasta", "cringe", "roast", "drama"]
        if any(kw in name or kw in title for kw in skip_keywords):
            continue

        promising.append(sr)

    # Sort by a combination of size and activity ratio
    promising.sort(
        key=lambda x: (x["subscribers"] * 0.4 + x["active_users"] * 100),
        reverse=True
    )

    return promising


# ── Trend Detection ────────────────────────────────────────────────────────────

def find_trending_topics(vertical_seeds: list[str]) -> list[dict]:
    """Find what's getting traction right now across seed communities."""
    trending = []

    for sub in vertical_seeds[:5]:
        url = f"https://www.reddit.com/r/{sub}/hot.json?limit=10"
        data = fetch_json(url, REDDIT_HEADERS)
        if not data:
            continue

        for child in data.get("data", {}).get("children", []):
            p = child.get("data", {})
            if p.get("stickied"):
                continue
            score = p.get("score", 0)
            if score > 100:
                trending.append({
                    "title":      p.get("title", ""),
                    "subreddit":  sub,
                    "score":      score,
                    "comments":   p.get("num_comments", 0),
                    "url":        f"https://reddit.com{p.get('permalink', '')}",
                })
        time.sleep(0.5)

    trending.sort(key=lambda x: x["score"], reverse=True)
    return trending[:20]


# ── Claude Opportunity Analysis ────────────────────────────────────────────────

def cluster_and_score_opportunities(
    subreddits: list[dict],
    gap_data: dict,
    vertical: str,
    vertical_config: dict,
) -> list[dict]:
    """
    Use Claude to cluster subreddits into content niches and score each opportunity.
    Returns ranked list of niche opportunities.
    """
    if not HAS_ANTHROPIC:
        warn("anthropic not installed — skipping AI clustering")
        return []

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        warn("ANTHROPIC_API_KEY not set")
        return []

    client = anthropic.Anthropic(api_key=api_key)

    # Build summary for Claude
    sr_summary = "\n".join([
        f"- r/{sr['name']} ({sr['subscribers']:,} subs, {sr['active_users']} active): {sr['description'][:120]}"
        for sr in subreddits[:40]
    ])

    gap_summary = "\n".join([
        f"- r/{name}: engagement={data.get('engagement_score', 0)}, gap_score={data.get('gap_score', 0)}, "
        f"top question: {data.get('top_questions', [{}])[0].get('title', 'N/A') if data.get('top_questions') else 'N/A'}"
        for name, data in list(gap_data.items())[:20]
    ])

    prompt = f"""You are a content strategy expert identifying profitable content site opportunities.

VERTICAL: {vertical}
YOUR ADVANTAGE IN THIS SPACE: {vertical_config.get('your_advantage', 'strong research capability')}
MONETIZATION MULTIPLIER: {vertical_config.get('monetization_multiplier', 1.0)}x

DISCOVERED SUBREDDITS:
{sr_summary}

CONTENT GAP ANALYSIS:
{gap_summary}

Your task: Identify the 8 best content site opportunities from this data.

A good opportunity has:
1. Clear audience with specific pain points
2. Content gap — questions asked but not well answered by existing sites
3. Monetization path — affiliate products, professional services, own products
4. Not dominated by one massive authoritative site already
5. Synergy with the stated advantages above

Respond with ONLY a JSON array (no markdown, no preamble):
[
  {{
    "niche_name": "<specific niche name, 2-4 words>",
    "slug": "<url-safe slug>",
    "primary_subreddits": ["<sub1>", "<sub2>"],
    "opportunity_score": <integer 1-100>,
    "score_rationale": "<why this score — specific, not generic>",
    "audience_size": "<small <50k | medium 50k-500k | large 500k-2M>",
    "content_gap": "<what's missing — be specific>",
    "monetization": {{
      "primary": "<method>",
      "estimated_rpm": "<$X-Y>",
      "specific_products": ["<affiliate product or own product idea>"]
    }},
    "synergy_with_existing": "<how this connects to lamphill/inside-bar/magpie network>",
    "top_article_ideas": [
      "<specific article title>",
      "<specific article title>",
      "<specific article title>"
    ],
    "competitive_risk": "<low | medium | high>",
    "time_to_traffic": "<3-6mo | 6-12mo | 12-24mo>",
    "verdict": "<STRONG_BUY | BUY | HOLD | PASS>",
    "one_liner": "<one sentence that sells this opportunity>"
  }}
]

Return exactly 8 opportunities, sorted by opportunity_score descending.
Be specific and contrarian — avoid obvious niches anyone would think of."""

    info("Clustering with Claude Sonnet...")

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}]
        )

        raw = response.content[0].text.strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)

        opportunities = json.loads(raw)
        ok(f"Identified {len(opportunities)} opportunities")
        return opportunities

    except json.JSONDecodeError as e:
        warn(f"Claude returned invalid JSON: {e}")
        return []
    except Exception as e:
        warn(f"Claude analysis failed: {e}")
        return []


# ── Report Generation ──────────────────────────────────────────────────────────

def generate_prospect_report(
    vertical: str,
    opportunities: list[dict],
    trending: list[dict],
    subreddit_count: int,
) -> str:

    lines = [
        f"---",
        f"vertical: {vertical}",
        f"scouted_at: {now_utc()}",
        f"opportunities_found: {len(opportunities)}",
        f"subreddits_analyzed: {subreddit_count}",
        f"---",
        f"",
        f"# Prospect Report: {vertical.title()} Vertical",
        f"*Generated {now_utc()}*",
        f"",
        f"## Summary",
        f"",
        f"Analyzed **{subreddit_count} subreddits** in the {vertical} vertical.",
        f"Found **{len(opportunities)} opportunities** ranked by score.",
        f"",
        f"| Rank | Niche | Score | Verdict | Time to Traffic |",
        f"|------|-------|-------|---------|----------------|",
    ]

    verdict_emoji = {"STRONG_BUY": "🟢", "BUY": "🔵", "HOLD": "🟡", "PASS": "🔴"}

    for i, opp in enumerate(opportunities[:8], 1):
        emoji = verdict_emoji.get(opp.get("verdict", ""), "⚪")
        lines.append(
            f"| {i} | {opp.get('niche_name', '')} | "
            f"{opp.get('opportunity_score', '?')}/100 | "
            f"{emoji} {opp.get('verdict', '')} | "
            f"{opp.get('time_to_traffic', '?')} |"
        )

    lines += ["", "---", ""]

    # Detail each opportunity
    for i, opp in enumerate(opportunities, 1):
        score   = opp.get("opportunity_score", 0)
        verdict = opp.get("verdict", "")
        emoji   = verdict_emoji.get(verdict, "⚪")

        lines += [
            f"## #{i}: {opp.get('niche_name', '')}",
            f"**{emoji} {verdict} — {score}/100**",
            f"",
            f"> {opp.get('one_liner', '')}",
            f"",
            f"**Primary communities:** {', '.join('r/' + s for s in opp.get('primary_subreddits', []))}",
            f"**Audience size:** {opp.get('audience_size', '?')}",
            f"**Competitive risk:** {opp.get('competitive_risk', '?')}",
            f"**Time to traffic:** {opp.get('time_to_traffic', '?')}",
            f"",
            f"### Content Gap",
            f"{opp.get('content_gap', '')}",
            f"",
            f"### Monetization",
            f"- **Primary:** {opp.get('monetization', {}).get('primary', '')}",
            f"- **Est. RPM:** {opp.get('monetization', {}).get('estimated_rpm', '')}",
        ]

        products = opp.get("monetization", {}).get("specific_products", [])
        if products:
            for p in products:
                lines.append(f"- {p}")

        lines += [
            f"",
            f"### Network Synergy",
            f"{opp.get('synergy_with_existing', '')}",
            f"",
            f"### Top Article Ideas",
        ]
        for article in opp.get("top_article_ideas", []):
            lines.append(f"- {article}")

        lines += [
            f"",
            f"### Why This Score",
            f"{opp.get('score_rationale', '')}",
            f"",
            f"---",
            f"",
        ]

    # Trending section
    if trending:
        lines += [
            f"## What's Trending Right Now",
            f"",
            f"Hot posts across {vertical} communities:",
            f"",
        ]
        for t in trending[:10]:
            lines.append(f"- [{t['score']} pts] **{t['title']}** (r/{t['subreddit']}, {t['comments']} comments)")

        lines += ["", "---", ""]

    # Next steps
    buy_opps = [o for o in opportunities if o.get("verdict") in ("STRONG_BUY", "BUY")]
    lines += [
        f"## Next Steps",
        f"",
    ]

    if buy_opps:
        lines += [
            f"Run deep validation on your top picks:",
            f"",
        ]
        for opp in buy_opps[:3]:
            slug = opp.get("slug", slugify(opp.get("niche_name", "")))
            lines += [
                f"```bash",
                f'python scout.py research --niche "{opp.get("niche_name", "")}" --depth deep',
                f"```",
                f"",
            ]
    else:
        lines += [
            f"No strong buys found in this vertical.",
            f"Try a different vertical or run with `--depth deep`.",
        ]

    return "\n".join(lines)


# ── Commands ───────────────────────────────────────────────────────────────────

def cmd_run(args):
    vertical = args.filter or "health"
    min_score = args.min_score
    limit     = args.limit

    if vertical not in VERTICALS:
        print(f"Unknown vertical '{vertical}'. Choose: {', '.join(VERTICALS)}")
        sys.exit(1)

    vertical_config = VERTICALS[vertical]
    seeds           = vertical_config["seeds"]

    print(f"\n{'═' * 60}")
    print(f"  PROSPECTOR — Autonomous Niche Discovery")
    print(f"  Vertical : {vertical}")
    print(f"  Seeds    : {len(seeds)} communities")
    print(f"{'═' * 60}")

    PROSPECTS_DIR.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Discover subreddits ────────────────────────────────────────────
    section(f"Step 1 — Discovering {vertical} communities")
    subreddits = discover_subreddits_from_seeds(seeds, limit_per_seed=4)
    subreddits = filter_promising_subreddits(subreddits)
    ok(f"Found {len(subreddits)} promising communities")

    # ── Step 2: Analyze content gaps ──────────────────────────────────────────
    section("Step 2 — Analyzing content gaps")
    gap_data = {}
    for sr in subreddits[:20]:  # Analyze top 20 by size
        name = sr.get("name", "")
        info(f"Analyzing r/{name}...")
        gap_data[name] = analyze_subreddit_content_gap(name)

    ok(f"Gap analysis complete for {len(gap_data)} communities")

    # ── Step 3: Find trending topics ──────────────────────────────────────────
    section("Step 3 — Trending topics")
    trending = find_trending_topics(seeds)
    ok(f"Found {len(trending)} trending posts")

    # ── Step 4: Claude clustering + scoring ───────────────────────────────────
    section("Step 4 — AI opportunity clustering")
    opportunities = cluster_and_score_opportunities(
        subreddits, gap_data, vertical, vertical_config
    )

    # Filter by min score
    opportunities = [o for o in opportunities if o.get("opportunity_score", 0) >= min_score]
    opportunities = opportunities[:limit]

    # ── Step 5: Save report ───────────────────────────────────────────────────
    section("Step 5 — Saving results")

    ts          = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_path = PROSPECTS_DIR / f"prospect_{vertical}_{ts}.md"
    json_path   = PROSPECTS_DIR / f"prospect_{vertical}_{ts}.json"

    report = generate_prospect_report(vertical, opportunities, trending, len(subreddits))
    report_path.write_text(report, encoding="utf-8")
    ok(f"Report: {report_path}")

    json_path.write_text(json.dumps({
        "vertical":      vertical,
        "run_at":        now_utc(),
        "subreddits":    subreddits,
        "gap_data":      gap_data,
        "trending":      trending,
        "opportunities": opportunities,
    }, indent=2, default=str), encoding="utf-8")
    ok(f"Raw data: {json_path}")

    # ── Summary ────────────────────────────────────────────────────────────────
    verdict_emoji = {"STRONG_BUY": "🟢", "BUY": "🔵", "HOLD": "🟡", "PASS": "🔴"}

    print(f"\n{'═' * 60}")
    print(f"  TOP OPPORTUNITIES — {vertical.upper()}")
    print(f"{'═' * 60}")
    print(f"  {'RANK':<5} {'NICHE':<32} {'SCORE':<7} {'VERDICT'}")
    print(f"  {'─'*5} {'─'*32} {'─'*7} {'─'*15}")

    for i, opp in enumerate(opportunities[:8], 1):
        emoji = verdict_emoji.get(opp.get("verdict", ""), "⚪")
        print(f"  #{i:<4} {opp.get('niche_name', ''):<32} "
              f"{opp.get('opportunity_score', '?'):<7} "
              f"{emoji} {opp.get('verdict', '')}")

    buy_opps = [o for o in opportunities if o.get("verdict") in ("STRONG_BUY", "BUY")]
    if buy_opps:
        print(f"\n  {len(buy_opps)} opportunities ready for deep validation.")
        print(f"  Run: python prospect.py pipeline --vertical {vertical}")

    print(f"\n  Full report: {report_path}\n")


def cmd_trends(args):
    """Show what's trending right now across all verticals."""
    section("Trending Now — Across All Verticals")

    all_trending = []
    for vertical, config in VERTICALS.items():
        info(f"Checking {vertical}...")
        trending = find_trending_topics(config["seeds"][:3])
        for t in trending[:5]:
            t["vertical"] = vertical
        all_trending.extend(trending[:5])
        time.sleep(1)

    all_trending.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n  {'SCORE':<8} {'VERTICAL':<15} {'TITLE'}")
    print(f"  {'─'*8} {'─'*15} {'─'*40}")
    for t in all_trending[:20]:
        title = t["title"][:55] + "..." if len(t["title"]) > 55 else t["title"]
        print(f"  {t['score']:<8} {t['vertical']:<15} {title}")


def cmd_gaps(args):
    """Deep gap analysis on a specific subreddit."""
    section(f"Content Gap Analysis: r/{args.subreddit}")

    sr_info = get_subreddit_info(args.subreddit)
    if sr_info:
        print(f"\n  Subscribers : {sr_info['subscribers']:,}")
        print(f"  Active now  : {sr_info['active_users']:,}")

    gap = analyze_subreddit_content_gap(args.subreddit)

    print(f"\n  Engagement score : {gap['engagement_score']}")
    print(f"  Content gap score: {gap['gap_score']}/100")

    if gap["top_questions"]:
        print(f"\n  Top unanswered questions:")
        for q in gap["top_questions"]:
            print(f"    [{q['comments']} comments] {q['title']}")

    print(f"\n  → Gap score {gap['gap_score']}/100 — ", end="")
    if gap["gap_score"] >= 60:
        print("Strong opportunity. Run scout.py research on this niche.")
    elif gap["gap_score"] >= 30:
        print("Moderate opportunity. Worth investigating.")
    else:
        print("Low gap signal. Community is well-served or low engagement.")


def cmd_pipeline(args):
    """Auto-pipe top prospects into scout.py for deep validation."""
    section("Auto-pipeline: Prospect → Scout")

    vertical = args.vertical or "health"

    # Find most recent prospect report for this vertical
    reports = sorted(PROSPECTS_DIR.glob(f"prospect_{vertical}_*.json"), reverse=True)
    if not reports:
        print(f"  No prospect reports found for '{vertical}'.")
        print(f"  Run: python prospect.py run --filter {vertical}")
        sys.exit(1)

    data = json.loads(reports[0].read_text(encoding="utf-8"))
    opportunities = data.get("opportunities", [])

    buy_opps = [
        o for o in opportunities
        if o.get("verdict") in ("STRONG_BUY", "BUY")
    ][:args.top]

    if not buy_opps:
        print("  No BUY-rated opportunities found. Adjust --min-score or run again.")
        sys.exit(0)

    print(f"\n  Piping {len(buy_opps)} opportunities to scout.py...\n")

    for opp in buy_opps:
        niche = opp.get("niche_name", "")
        print(f"  Scouting: {niche}")
        print(f"  {'─' * 50}")

        result = subprocess.run(
            [sys.executable, str(FACTORY_ROOT / "scout.py"),
             "research", "--niche", niche, "--depth", args.depth],
            cwd=str(FACTORY_ROOT),
        )

        if result.returncode != 0:
            warn(f"Scout failed for: {niche}")

        time.sleep(2)

    print(f"\n  Done. Check pipeline/scouts/ for detailed reports.")
    print(f"  Then provision your top picks:")
    print(f"    python provision.py new --site <slug> --domain <domain> --niche <niche-preset>")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Autonomous niche discovery for Article Factory",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command")

    # run
    p_run = sub.add_parser("run", help="Full discovery run for a vertical")
    p_run.add_argument("--filter",    default="health",
                       choices=list(VERTICALS.keys()),
                       help="Vertical to prospect")
    p_run.add_argument("--min-score", type=int, default=40,
                       help="Minimum opportunity score to include (default: 40)")
    p_run.add_argument("--limit",     type=int, default=8,
                       help="Max opportunities to return (default: 8)")

    # trends
    sub.add_parser("trends", help="What's trending right now across all verticals")

    # gaps
    p_gaps = sub.add_parser("gaps", help="Deep gap analysis on a specific subreddit")
    p_gaps.add_argument("--subreddit", required=True)

    # pipeline
    p_pipe = sub.add_parser("pipeline", help="Auto-pipe top prospects into scout.py")
    p_pipe.add_argument("--vertical", default="health",
                        choices=list(VERTICALS.keys()))
    p_pipe.add_argument("--top",   type=int, default=3,
                        help="Number of top opportunities to scout (default: 3)")
    p_pipe.add_argument("--depth", default="standard",
                        choices=["quick", "standard", "deep"])

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    dispatch = {
        "run":      cmd_run,
        "trends":   cmd_trends,
        "gaps":     cmd_gaps,
        "pipeline": cmd_pipeline,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()