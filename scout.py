#!/usr/bin/env python3
"""
scout.py - Niche research and validation module for Article Factory

Researches Reddit and YouTube to validate a niche before provisioning a site.
Outputs a viability score, content gap analysis, and seed topics ready for
the article factory pipeline.

Usage:
    python scout.py research --niche "cold plunge therapy"
    python scout.py research --niche "lab medicine careers" --depth deep
    python scout.py research --niche "sleep optimization" --output pipeline/scouts/
    python scout.py list                          # Show all past scouting reports
    python scout.py compare sleep-optimization cold-plunge-therapy

Options:
    --niche     Niche idea to research (natural language)
    --depth     quick | standard | deep (default: standard)
    --output    Output directory (default: pipeline/scouts/)
    --no-reddit Skip Reddit research
    --no-youtube Skip YouTube research
"""

import argparse
import json
import os
import re
import sys
import time
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
FACTORY_ROOT = Path(__file__).parent
SCOUTS_DIR   = FACTORY_ROOT / "pipeline" / "scouts"

# ── Config ─────────────────────────────────────────────────────────────────────
REDDIT_HEADERS = {
    "User-Agent": "ArticleFactoryScout/1.0 (research tool; contact via github)"
}

DEPTH_CONFIG = {
    "quick":    {"subreddits": 3, "posts_per_sub": 10, "yt_results": 10, "model": "claude-haiku-4-5-20251001"},
    "standard": {"subreddits": 5, "posts_per_sub": 25, "yt_results": 20, "model": "claude-sonnet-4-20250514"},
    "deep":     {"subreddits": 8, "posts_per_sub": 50, "yt_results": 40, "model": "claude-sonnet-4-20250514"},
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
    """Fetch JSON from URL with retry logic."""
    req = urllib.request.Request(url, headers=headers or {})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** attempt
                info(f"Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif e.code == 404:
                return None
            else:
                warn(f"HTTP {e.code} for {url}")
                return None
        except Exception as e:
            if attempt == retries - 1:
                warn(f"Failed to fetch {url}: {e}")
            time.sleep(1)
    return None

# ── Reddit Research ────────────────────────────────────────────────────────────

def find_subreddits(niche: str, limit: int = 8) -> list[dict]:
    """Find relevant subreddits for a niche."""
    info(f"Searching Reddit for subreddits related to: {niche}")
    url = f"https://www.reddit.com/search.json?q={quote_plus(niche)}&type=sr&limit={limit}"
    data = fetch_json(url, REDDIT_HEADERS)
    if not data:
        return []

    subreddits = []
    for child in data.get("data", {}).get("children", []):
        sr = child.get("data", {})
        subreddits.append({
            "name":        sr.get("display_name", ""),
            "title":       sr.get("title", ""),
            "subscribers": sr.get("subscribers", 0),
            "description": (sr.get("public_description") or sr.get("description") or "")[:300],
            "url":         f"https://reddit.com/r/{sr.get('display_name', '')}",
        })

    # Also try direct subreddit search by keyword variations
    keywords = niche.lower().replace(" ", "")
    direct_url = f"https://www.reddit.com/r/{keywords}/about.json"
    direct = fetch_json(direct_url, REDDIT_HEADERS)
    if direct and direct.get("data", {}).get("display_name"):
        sr = direct["data"]
        direct_entry = {
            "name":        sr.get("display_name", ""),
            "title":       sr.get("title", ""),
            "subscribers": sr.get("subscribers", 0),
            "description": (sr.get("public_description") or "")[:300],
            "url":         f"https://reddit.com/r/{sr.get('display_name', '')}",
        }
        if not any(s["name"] == direct_entry["name"] for s in subreddits):
            subreddits.insert(0, direct_entry)

    # Sort by subscribers
    subreddits.sort(key=lambda x: x["subscribers"], reverse=True)
    ok(f"Found {len(subreddits)} subreddits")
    return subreddits


def get_top_posts(subreddit: str, limit: int = 25) -> list[dict]:
    """Get top posts from a subreddit."""
    posts = []
    for timeframe in ["month", "year"]:
        url = f"https://www.reddit.com/r/{subreddit}/top.json?t={timeframe}&limit={limit}"
        data = fetch_json(url, REDDIT_HEADERS)
        if not data:
            continue
        for child in data.get("data", {}).get("children", []):
            p = child.get("data", {})
            if p.get("stickied"):
                continue
            posts.append({
                "title":     p.get("title", ""),
                "score":     p.get("score", 0),
                "comments":  p.get("num_comments", 0),
                "url":       p.get("url", ""),
                "selftext":  (p.get("selftext") or "")[:500],
                "flair":     p.get("link_flair_text") or "",
            })
        time.sleep(0.5)  # Be polite to Reddit

    # Deduplicate and sort by engagement
    seen = set()
    unique = []
    for p in posts:
        if p["title"] not in seen:
            seen.add(p["title"])
            unique.append(p)

    unique.sort(key=lambda x: x["score"] + x["comments"] * 3, reverse=True)
    return unique[:limit]


def get_hot_questions(subreddit: str, limit: int = 15) -> list[dict]:
    """Get recent questions from a subreddit."""
    url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}"
    data = fetch_json(url, REDDIT_HEADERS)
    if not data:
        return []

    questions = []
    for child in data.get("data", {}).get("children", []):
        p = child.get("data", {})
        title = p.get("title", "")
        # Filter for question-like posts
        if any(w in title.lower() for w in ["?", "how", "what", "why", "best", "recommend", "advice"]):
            questions.append({
                "title":    title,
                "score":    p.get("score", 0),
                "comments": p.get("num_comments", 0),
            })
    return questions


def research_reddit(niche: str, config: dict) -> dict:
    """Full Reddit research for a niche."""
    section("Reddit Research")
    results = {
        "subreddits": [],
        "top_posts":  [],
        "questions":  [],
        "total_community_size": 0,
    }

    subreddits = find_subreddits(niche, limit=config["subreddits"] + 3)
    subreddits = subreddits[:config["subreddits"]]
    results["subreddits"] = subreddits
    results["total_community_size"] = sum(s["subscribers"] for s in subreddits)

    for sr in subreddits:
        name = sr["name"]
        info(f"Scraping r/{name} ({sr['subscribers']:,} subscribers)...")

        posts = get_top_posts(name, config["posts_per_sub"])
        results["top_posts"].extend(posts[:10])

        questions = get_hot_questions(name, 15)
        results["questions"].extend(questions[:5])

        time.sleep(1)  # Polite delay

    # Sort and deduplicate
    results["top_posts"].sort(key=lambda x: x["score"], reverse=True)
    results["top_posts"] = results["top_posts"][:30]

    ok(f"Reddit: {len(results['subreddits'])} subreddits, "
       f"{len(results['top_posts'])} posts, "
       f"{results['total_community_size']:,} total subscribers")

    return results


# ── YouTube Research ───────────────────────────────────────────────────────────

def research_youtube(niche: str, config: dict) -> dict:
    """Research YouTube for niche content patterns."""
    section("YouTube Research")

    api_key = os.getenv("YOUTUBE_API_KEY")
    results = {
        "videos":   [],
        "channels": [],
        "available": bool(api_key),
    }

    if not api_key:
        warn("YOUTUBE_API_KEY not set — skipping YouTube research")
        warn("Add to factory/.env for YouTube analysis")
        return results

    base = "https://www.googleapis.com/youtube/v3"

    # Search for videos
    params = {
        "part":       "snippet",
        "q":          niche,
        "type":       "video",
        "order":      "viewCount",
        "maxResults": min(config["yt_results"], 50),
        "key":        api_key,
        "relevanceLanguage": "en",
    }
    url = f"{base}/search?{urlencode(params)}"
    data = fetch_json(url)

    if not data or "items" not in data:
        warn("YouTube API returned no results")
        return results

    video_ids = [item["id"]["videoId"] for item in data["items"]
                 if item.get("id", {}).get("videoId")]

    # Get video statistics
    if video_ids:
        stats_params = {
            "part": "statistics,snippet",
            "id":   ",".join(video_ids[:50]),
            "key":  api_key,
        }
        stats_url = f"{base}/videos?{urlencode(stats_params)}"
        stats_data = fetch_json(stats_url)

        if stats_data and "items" in stats_data:
            for item in stats_data["items"]:
                snippet = item.get("snippet", {})
                stats   = item.get("statistics", {})
                results["videos"].append({
                    "title":       snippet.get("title", ""),
                    "channel":     snippet.get("channelTitle", ""),
                    "channel_id":  snippet.get("channelId", ""),
                    "views":       int(stats.get("viewCount", 0)),
                    "likes":       int(stats.get("likeCount", 0)),
                    "comments":    int(stats.get("commentCount", 0)),
                    "published":   snippet.get("publishedAt", "")[:10],
                    "description": snippet.get("description", "")[:300],
                })

    # Sort by views
    results["videos"].sort(key=lambda x: x["views"], reverse=True)

    # Extract unique channels
    channel_map = {}
    for v in results["videos"]:
        cid = v["channel_id"]
        if cid not in channel_map:
            channel_map[cid] = {"name": v["channel"], "videos": 0, "total_views": 0}
        channel_map[cid]["videos"] += 1
        channel_map[cid]["total_views"] += v["views"]

    results["channels"] = sorted(
        [{"id": k, **v} for k, v in channel_map.items()],
        key=lambda x: x["total_views"],
        reverse=True
    )[:10]

    ok(f"YouTube: {len(results['videos'])} videos, {len(results['channels'])} channels")
    return results


# ── Claude Analysis ────────────────────────────────────────────────────────────

def analyze_with_claude(niche: str, reddit: dict, youtube: dict, model: str) -> dict:
    """Use Claude to analyze research data and generate insights."""
    section("AI Analysis")

    if not HAS_ANTHROPIC:
        warn("anthropic package not installed — skipping AI analysis")
        return {}

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        warn("ANTHROPIC_API_KEY not set — skipping AI analysis")
        return {}

    client = anthropic.Anthropic(api_key=api_key)

    # Build research summary for Claude
    reddit_summary = f"""
REDDIT RESEARCH FOR: {niche}

Communities found:
{chr(10).join(f"- r/{s['name']}: {s['subscribers']:,} subscribers — {s['description'][:100]}"
              for s in reddit.get('subreddits', [])[:5])}

Total community size: {reddit.get('total_community_size', 0):,} subscribers

Top performing posts (by engagement):
{chr(10).join(f"- [{p['score']} pts, {p['comments']} comments] {p['title']}"
              for p in reddit.get('top_posts', [])[:15])}

Recurring questions people ask:
{chr(10).join(f"- {q['title']}" for q in reddit.get('questions', [])[:10])}
"""

    yt_summary = ""
    if youtube.get("available") and youtube.get("videos"):
        yt_summary = f"""
YOUTUBE RESEARCH:

Top channels covering this niche:
{chr(10).join(f"- {c['name']}: {c['total_views']:,} total views across {c['videos']} videos"
              for c in youtube.get('channels', [])[:5])}

Top performing videos:
{chr(10).join(f"- [{v['views']:,} views] {v['title']} — {v['channel']}"
              for v in youtube.get('videos', [])[:10])}
"""
    else:
        yt_summary = "\nYOUTUBE RESEARCH: Not available for this analysis.\n"

    prompt = f"""You are a content strategist and SEO expert analyzing a potential niche for a content site.

{reddit_summary}
{yt_summary}

Analyze this niche and respond with ONLY a JSON object (no markdown, no preamble) in this exact structure:

{{
  "viability_score": <integer 1-100>,
  "score_breakdown": {{
    "community_size": <integer 1-25, score for size of Reddit communities>,
    "content_gap": <integer 1-25, how underserved is quality content>,
    "monetization": <integer 1-25, commercial intent and affiliate/product potential>,
    "competition": <integer 1-25, inverse of competition — higher = less competitive>
  }},
  "verdict": "<STRONG_BUY | BUY | HOLD | PASS>",
  "verdict_reason": "<2-3 sentence explanation of verdict>",
  "top_pain_points": [
    "<pain point 1 — specific, not generic>",
    "<pain point 2>",
    "<pain point 3>",
    "<pain point 4>",
    "<pain point 5>"
  ],
  "content_gaps": [
    "<content type or topic that is underserved>",
    "<gap 2>",
    "<gap 3>"
  ],
  "monetization_paths": [
    {{
      "method": "<affiliate | own_product | lead_gen | ads | sponsorship>",
      "description": "<specific opportunity>",
      "estimated_rpm": "<low $1-5 | medium $5-20 | high $20-50 | very_high $50+>"
    }}
  ],
  "seed_topics": [
    {{
      "title": "<article title — specific, SEO-optimized, sounds human>",
      "type": "<deep_dive | listicle | guide | comparison | review>",
      "pain_point": "<which pain point this addresses>",
      "estimated_search_intent": "<informational | commercial | transactional>"
    }}
  ],
  "cluster_synergies": [
    "<how this site could link to or complement existing sites in the network>"
  ],
  "recommended_voice": "<1 sentence describing the ideal author persona for this niche>",
  "recommended_monetization_tier": "<flagship | authority | satellite | micro>",
  "risks": [
    "<risk 1>",
    "<risk 2>"
  ]
}}

Generate exactly 10 seed_topics. Make them specific, not generic. Think like an expert in this field who knows what people actually want to read."""

    info(f"Analyzing with {model}...")

    try:
        response = client.messages.create(
            model=model,
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}]
        )

        raw = response.content[0].text.strip()

        # Strip markdown fences if present
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)

        analysis = json.loads(raw)
        ok(f"Analysis complete — viability score: {analysis.get('viability_score')}/100 "
           f"({analysis.get('verdict')})")
        return analysis

    except json.JSONDecodeError as e:
        warn(f"Claude returned invalid JSON: {e}")
        warn("Raw response saved to debug — partial results may be available")
        return {"raw_response": raw, "error": str(e)}
    except Exception as e:
        warn(f"Claude analysis failed: {e}")
        return {}


# ── Report Generation ──────────────────────────────────────────────────────────

def generate_report(niche: str, reddit: dict, youtube: dict, analysis: dict) -> str:
    """Generate a markdown scouting report."""

    score       = analysis.get("viability_score", "N/A")
    verdict     = analysis.get("verdict", "UNKNOWN")
    breakdown   = analysis.get("score_breakdown", {})

    verdict_emoji = {
        "STRONG_BUY": "🟢",
        "BUY":        "🔵",
        "HOLD":       "🟡",
        "PASS":       "🔴",
    }.get(verdict, "⚪")

    lines = [
        f"---",
        f"niche: \"{niche}\"",
        f"slug: {slugify(niche)}",
        f"scouted_at: {now_utc()}",
        f"viability_score: {score}",
        f"verdict: {verdict}",
        f"---",
        f"",
        f"# Scout Report: {niche}",
        f"",
        f"## {verdict_emoji} Verdict: {verdict} — {score}/100",
        f"",
        f"{analysis.get('verdict_reason', '')}",
        f"",
        f"### Score Breakdown",
        f"",
        f"| Dimension | Score | Max |",
        f"|-----------|-------|-----|",
        f"| Community Size | {breakdown.get('community_size', '?')} | 25 |",
        f"| Content Gap | {breakdown.get('content_gap', '?')} | 25 |",
        f"| Monetization Potential | {breakdown.get('monetization', '?')} | 25 |",
        f"| Competition (inverse) | {breakdown.get('competition', '?')} | 25 |",
        f"| **Total** | **{score}** | **100** |",
        f"",
        f"---",
        f"",
        f"## Reddit Communities",
        f"",
        f"**Total community size: {reddit.get('total_community_size', 0):,} subscribers**",
        f"",
    ]

    for sr in reddit.get("subreddits", [])[:5]:
        lines.append(f"- **r/{sr['name']}** — {sr['subscribers']:,} subscribers")
        if sr.get("description"):
            lines.append(f"  {sr['description'][:120]}")

    lines += [
        f"",
        f"### Top Performing Posts",
        f"",
    ]
    for p in reddit.get("top_posts", [])[:10]:
        lines.append(f"- [{p['score']} pts] **{p['title']}** ({p['comments']} comments)")

    if youtube.get("available") and youtube.get("videos"):
        lines += [
            f"",
            f"---",
            f"",
            f"## YouTube Landscape",
            f"",
            f"### Top Channels",
            f"",
        ]
        for c in youtube.get("channels", [])[:5]:
            lines.append(f"- **{c['name']}** — {c['total_views']:,} total views")

        lines += [f"", f"### Top Videos", f""]
        for v in youtube.get("videos", [])[:8]:
            lines.append(f"- [{v['views']:,} views] **{v['title']}** — {v['channel']}")

    lines += [
        f"",
        f"---",
        f"",
        f"## Pain Points",
        f"",
        f"What this audience consistently struggles with:",
        f"",
    ]
    for pp in analysis.get("top_pain_points", []):
        lines.append(f"- {pp}")

    lines += [
        f"",
        f"## Content Gaps",
        f"",
        f"What's underserved and worth targeting:",
        f"",
    ]
    for gap in analysis.get("content_gaps", []):
        lines.append(f"- {gap}")

    lines += [
        f"",
        f"## Monetization Paths",
        f"",
        f"**Recommended tier: {analysis.get('recommended_monetization_tier', 'TBD')}**",
        f"",
    ]
    for mp in analysis.get("monetization_paths", []):
        lines.append(f"### {mp.get('method', '').replace('_', ' ').title()}")
        lines.append(f"{mp.get('description', '')}")
        lines.append(f"Estimated RPM: {mp.get('estimated_rpm', 'unknown')}")
        lines.append(f"")

    lines += [
        f"## Network Synergies",
        f"",
    ]
    for syn in analysis.get("cluster_synergies", []):
        lines.append(f"- {syn}")

    lines += [
        f"",
        f"## Voice & Persona",
        f"",
        f"{analysis.get('recommended_voice', '')}",
        f"",
        f"---",
        f"",
        f"## Risks",
        f"",
    ]
    for risk in analysis.get("risks", []):
        lines.append(f"- {risk}")

    lines += [
        f"",
        f"---",
        f"",
        f"## Seed Topics (10)",
        f"",
        f"Ready to feed into topic_generator.py:",
        f"",
    ]

    for i, topic in enumerate(analysis.get("seed_topics", []), 1):
        lines += [
            f"### {i}. {topic.get('title', '')}",
            f"- **Type:** {topic.get('type', '')}",
            f"- **Addresses:** {topic.get('pain_point', '')}",
            f"- **Intent:** {topic.get('estimated_search_intent', '')}",
            f"",
        ]

    lines += [
        f"---",
        f"",
        f"## Next Steps",
        f"",
    ]

    if verdict in ("STRONG_BUY", "BUY"):
        lines += [
            f"This niche scores high enough to provision. Run:",
            f"",
            f"```bash",
            f"python provision.py new \\",
            f"  --site {slugify(niche)} \\",
            f"  --domain {slugify(niche)}.com \\",
            f"  --niche <niche-preset> \\",
            f"  --dry-run",
            f"```",
            f"",
            f"Then seed the pipeline with the 10 topics above.",
        ]
    elif verdict == "HOLD":
        lines += [
            f"Consider researching further before committing.",
            f"Run with `--depth deep` to get more data.",
        ]
    else:
        lines += [
            f"This niche doesn't meet the threshold. Consider adjacent niches or run",
            f"`scout.py research` on a related variation.",
        ]

    return "\n".join(lines)


def save_seed_topics(niche: str, analysis: dict, output_dir: Path) -> Path | None:
    """Save seed topics in factory-compatible format."""
    topics = analysis.get("seed_topics", [])
    if not topics:
        return None

    slug = slugify(niche)
    ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"seeds_{slug}_{ts}.json"

    payload = {
        "source":    "scout",
        "niche":     niche,
        "generated": now_utc(),
        "topics":    [
            {
                "title": t.get("title", ""),
                "type":  t.get("type", "deep_dive"),
                "notes": t.get("pain_point", ""),
            }
            for t in topics
        ]
    }

    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


# ── Commands ───────────────────────────────────────────────────────────────────

def cmd_research(args):
    niche      = args.niche
    depth      = args.depth
    config     = DEPTH_CONFIG[depth]
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'═' * 60}")
    print(f"  SCOUT — Niche Research")
    print(f"  Niche : {niche}")
    print(f"  Depth : {depth}")
    print(f"{'═' * 60}")

    # ── Reddit ─────────────────────────────────────────────────────────────────
    reddit = {}
    if not args.no_reddit:
        reddit = research_reddit(niche, config)
    else:
        info("Skipping Reddit (--no-reddit)")

    # ── YouTube ────────────────────────────────────────────────────────────────
    youtube = {}
    if not args.no_youtube:
        youtube = research_youtube(niche, config)
    else:
        info("Skipping YouTube (--no-youtube)")

    # ── Claude Analysis ────────────────────────────────────────────────────────
    analysis = analyze_with_claude(niche, reddit, youtube, config["model"])

    # ── Save Report ────────────────────────────────────────────────────────────
    section("Saving Results")

    slug = slugify(niche)
    ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Markdown report
    report_path = output_dir / f"scout_{slug}_{ts}.md"
    report      = generate_report(niche, reddit, youtube, analysis)
    report_path.write_text(report, encoding="utf-8")
    ok(f"Report saved: {report_path}")

    # Raw JSON data
    json_path = output_dir / f"scout_{slug}_{ts}.json"
    json_path.write_text(json.dumps({
        "niche":    niche,
        "depth":    depth,
        "scouted":  now_utc(),
        "reddit":   reddit,
        "youtube":  youtube,
        "analysis": analysis,
    }, indent=2, default=str), encoding="utf-8")
    ok(f"Raw data saved: {json_path}")

    # Seed topics for pipeline
    if analysis.get("seed_topics"):
        seeds_path = save_seed_topics(niche, analysis, output_dir)
        ok(f"Seed topics saved: {seeds_path}")

    # ── Summary ────────────────────────────────────────────────────────────────
    score   = analysis.get("viability_score", "N/A")
    verdict = analysis.get("verdict", "UNKNOWN")
    verdict_emoji = {
        "STRONG_BUY": "🟢", "BUY": "🔵", "HOLD": "🟡", "PASS": "🔴"
    }.get(verdict, "⚪")

    print(f"\n{'═' * 60}")
    print(f"  {verdict_emoji}  VERDICT: {verdict}  —  {score}/100")
    print(f"{'═' * 60}")
    print(f"\n  {analysis.get('verdict_reason', '')}\n")

    if analysis.get("top_pain_points"):
        print("  Top pain points:")
        for pp in analysis["top_pain_points"][:3]:
            print(f"    • {pp}")

    if analysis.get("seed_topics"):
        print(f"\n  First 3 seed topics:")
        for t in analysis["seed_topics"][:3]:
            print(f"    • {t.get('title', '')}")

    print(f"\n  Full report: {report_path}\n")


def cmd_list(args):
    section("Past Scouting Reports")
    scouts_dir = SCOUTS_DIR
    if not scouts_dir.exists():
        print("  No scouting reports found.")
        return

    reports = sorted(scouts_dir.glob("scout_*.md"), reverse=True)
    if not reports:
        print("  No scouting reports found.")
        return

    print(f"  {'NICHE':<35} {'SCORE':<8} {'VERDICT':<15} {'DATE'}")
    print(f"  {'─'*35} {'─'*8} {'─'*15} {'─'*12}")

    for r in reports:
        content = r.read_text(encoding="utf-8")
        # Parse frontmatter
        niche   = re.search(r'niche: "(.+?)"', content)
        score   = re.search(r'viability_score: (\d+)', content)
        verdict = re.search(r'verdict: (\w+)', content)
        date    = re.search(r'scouted_at: (\S+)', content)

        niche_str   = niche.group(1)[:33] if niche else r.stem[:33]
        score_str   = score.group(1) if score else "?"
        verdict_str = verdict.group(1) if verdict else "?"
        date_str    = date.group(1)[:10] if date else "?"

        emoji = {"STRONG_BUY": "🟢", "BUY": "🔵", "HOLD": "🟡", "PASS": "🔴"}.get(verdict_str, "⚪")
        print(f"  {niche_str:<35} {score_str:<8} {emoji} {verdict_str:<13} {date_str}")

    print(f"\n  Total: {len(reports)} report(s) in {scouts_dir}")


def cmd_compare(args):
    section(f"Comparing: {' vs '.join(args.slugs)}")
    scouts_dir = SCOUTS_DIR

    results = []
    for slug in args.slugs:
        matches = sorted(scouts_dir.glob(f"scout_{slug}_*.json"), reverse=True)
        if not matches:
            warn(f"No report found for: {slug}")
            continue
        data = json.loads(matches[0].read_text(encoding="utf-8"))
        analysis = data.get("analysis", {})
        results.append({
            "niche":   data.get("niche", slug),
            "score":   analysis.get("viability_score", 0),
            "verdict": analysis.get("verdict", "?"),
            "breakdown": analysis.get("score_breakdown", {}),
            "tier":    analysis.get("recommended_monetization_tier", "?"),
        })

    if not results:
        print("  No reports to compare.")
        return

    results.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n  {'NICHE':<35} {'SCORE':<8} {'VERDICT':<15} {'TIER'}")
    print(f"  {'─'*35} {'─'*8} {'─'*15} {'─'*12}")
    for r in results:
        emoji = {"STRONG_BUY": "🟢", "BUY": "🔵", "HOLD": "🟡", "PASS": "🔴"}.get(r["verdict"], "⚪")
        print(f"  {r['niche']:<35} {r['score']:<8} {emoji} {r['verdict']:<13} {r['tier']}")

    print(f"\n  Winner: {results[0]['niche']} ({results[0]['score']}/100)")


# ── Entry Point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scout niches before provisioning sites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command")

    # research
    p_research = sub.add_parser("research", help="Research a niche")
    p_research.add_argument("--niche",      required=True, help="Niche to research")
    p_research.add_argument("--depth",      default="standard",
                            choices=["quick", "standard", "deep"])
    p_research.add_argument("--output",     default=str(SCOUTS_DIR))
    p_research.add_argument("--no-reddit",  action="store_true")
    p_research.add_argument("--no-youtube", action="store_true")

    # list
    sub.add_parser("list", help="List past scouting reports")

    # compare
    p_compare = sub.add_parser("compare", help="Compare scouting reports")
    p_compare.add_argument("slugs", nargs="+", help="Slugified niche names to compare")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    dispatch = {
        "research": cmd_research,
        "list":     cmd_list,
        "compare":  cmd_compare,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
