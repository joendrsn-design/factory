"""
============================================================
ARTICLE FACTORY — SITE LOADER
============================================================
Shared configuration module. Every factory module imports this
to get site context. Single source of truth for all site DNA.

Usage:
    from site_loader import SiteLoader

    loader = SiteLoader(config_dir="config/sites")

    # Load full site context
    site = loader.load("lamphill")

    # Get specific article type config
    article_type = loader.get_article_type("lamphill", "deep_dive")

    # List all available sites
    sites = loader.list_sites()

    # List sites that share research with a given site
    partners = loader.get_research_partners("lamphill")

    # Get the write prompt context (everything Write module needs)
    write_ctx = loader.get_write_context("lamphill", "deep_dive")

    # Get the QA context (everything QA module needs)
    qa_ctx = loader.get_qa_context("lamphill", "deep_dive")

    # Validate all configs on startup
    report = loader.validate_all()
============================================================
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Any, Optional
from dataclasses import dataclass, field
from datetime import datetime

# ── Logging ─────────────────────────────────────────────────
logger = logging.getLogger("article_factory.site_loader")


# ── Required Fields (validation) ────────────────────────────
REQUIRED_TOP_LEVEL = [
    "site_id", "site_name", "domain", "tier", "niche"
]

REQUIRED_VOICE = [
    "tone", "persona", "pov", "reading_level"
]

REQUIRED_ARTICLE_TYPE = [
    "type_id", "label", "word_count_min", "word_count_max",
    "research_depth", "enabled"
]

VALID_TIERS = {"flagship", "daily", "network", "authority"}
VALID_RESEARCH_DEPTHS = {"shallow", "moderate", "deep", "exhaustive"}
VALID_SEO_PRIORITIES = {"high", "medium", "low"}
VALID_POV = {"first_person", "first_person_authority", "third_person", "third_person_professional", "second_person_instructional", "second_person_reflective"}


# ── Exceptions ──────────────────────────────────────────────
class SiteConfigError(Exception):
    """Raised when a site config is invalid or missing."""
    pass


class SiteNotFoundError(SiteConfigError):
    """Raised when a requested site config doesn't exist."""
    pass


class ArticleTypeNotFoundError(SiteConfigError):
    """Raised when a requested article type doesn't exist for a site."""
    pass


# ── Site Context (what gets passed to modules) ──────────────
@dataclass
class SiteContext:
    """
    Immutable site context object passed through the pipeline.
    Every module receives this. Nobody modifies it.
    Think of it as the patient wristband — identifies the site
    and carries all instructions through every stage.
    """
    # Identity
    site_id: str
    site_name: str
    domain: str
    tier: str
    niche: str
    sub_niche: str = ""

    # Audience
    audience: dict = field(default_factory=dict)

    # Voice
    voice: dict = field(default_factory=dict)

    # Article Types
    article_types: list = field(default_factory=list)

    # Categories
    categories: list = field(default_factory=list)

    # SEO
    seo: dict = field(default_factory=dict)

    # Products
    products: dict = field(default_factory=dict)

    # Quality
    quality: dict = field(default_factory=dict)

    # Research
    research: dict = field(default_factory=dict)

    # Output
    output: dict = field(default_factory=dict)

    # Raw config (full YAML dict, for anything not explicitly mapped)
    _raw: dict = field(default_factory=dict, repr=False)

    def get_article_type(self, type_id: str) -> Optional[dict]:
        """Get a specific article type config by type_id."""
        for at in self.article_types:
            if at.get("type_id") == type_id:
                return at
        return None

    def get_enabled_article_types(self) -> list:
        """Get all enabled article types for this site."""
        return [at for at in self.article_types if at.get("enabled", True)]

    def get_article_type_ids(self) -> list:
        """Get list of all enabled article type IDs."""
        return [at["type_id"] for at in self.get_enabled_article_types()]

    def get_category_slugs(self) -> list:
        """Get list of all category slugs for this site."""
        return [cat.get("slug", "") for cat in self.categories if cat.get("slug")]

    def get_category_by_slug(self, slug: str) -> Optional[dict]:
        """Get a category by its slug."""
        for cat in self.categories:
            if cat.get("slug") == slug:
                return cat
        return None


# ── Site Loader ─────────────────────────────────────────────
class SiteLoader:
    """
    Loads, validates, and serves site configurations.
    Instantiate once, use everywhere.

        loader = SiteLoader(config_dir="config/sites")
        site = loader.load("lamphill")
    """

    def __init__(self, config_dir: str = "config/sites"):
        self.config_dir = Path(config_dir)
        self._cache: dict[str, SiteContext] = {}

        if not self.config_dir.exists():
            raise SiteConfigError(
                f"Config directory not found: {self.config_dir}"
            )

        logger.info(f"SiteLoader initialized. Config dir: {self.config_dir}")

    # ── Core Loading ────────────────────────────────────────

    def load(self, site_id: str, force_reload: bool = False) -> SiteContext:
        """
        Load a site config by site_id. Returns cached version
        unless force_reload=True.
        """
        if site_id in self._cache and not force_reload:
            return self._cache[site_id]

        config_path = self._find_config(site_id)
        raw = self._read_yaml(config_path)
        self._validate(raw, config_path)
        context = self._build_context(raw)

        self._cache[site_id] = context
        logger.info(f"Loaded site config: {site_id}")
        return context

    def load_all(self) -> dict[str, SiteContext]:
        """Load all site configs. Returns dict of site_id -> SiteContext."""
        sites = {}
        for site_id in self.list_sites():
            try:
                sites[site_id] = self.load(site_id)
            except SiteConfigError as e:
                logger.error(f"Failed to load {site_id}: {e}")
        return sites

    # ── Convenience Methods (what modules actually call) ────

    def get_article_type(self, site_id: str, type_id: str) -> dict:
        """Get a specific article type config. Raises if not found."""
        site = self.load(site_id)
        at = site.get_article_type(type_id)
        if at is None:
            raise ArticleTypeNotFoundError(
                f"Article type '{type_id}' not found for site '{site_id}'. "
                f"Available: {site.get_article_type_ids()}"
            )
        return at

    def get_write_context(self, site_id: str, type_id: str) -> dict:
        """
        Get everything the Write module needs in one call.
        Voice + audience + article type + product rules + SEO.
        """
        site = self.load(site_id)
        article_type = self.get_article_type(site_id, type_id)

        return {
            "site_id": site.site_id,
            "site_name": site.site_name,
            "voice": site.voice,
            "audience": site.audience,
            "article_type": article_type,
            "seo": site.seo,
            "products": site.products,
        }

    def get_research_context(self, site_id: str, type_id: str) -> dict:
        """
        Get everything the Research module needs.
        Niche + research depth + vault tags + shared research partners.
        """
        site = self.load(site_id)
        article_type = self.get_article_type(site_id, type_id)

        return {
            "site_id": site.site_id,
            "niche": site.niche,
            "sub_niche": site.sub_niche,
            "audience": site.audience,
            "research_depth": article_type.get("research_depth", "moderate"),
            "citation_required": article_type.get("citation_required", False),
            "vault_tags": site.research.get("vault_tags", []),
            "max_research_age_days": site.research.get("max_research_age_days", 30),
            "shared_with": site.research.get("shared_with", []),
        }

    def get_planning_context(self, site_id: str, type_id: str) -> dict:
        """
        Get everything the Planning module needs.
        Article type structure + voice + audience + SEO.
        """
        site = self.load(site_id)
        article_type = self.get_article_type(site_id, type_id)

        return {
            "site_id": site.site_id,
            "site_name": site.site_name,
            "voice": site.voice,
            "audience": site.audience,
            "article_type": article_type,
            "seo": site.seo,
        }

    def get_qa_context(self, site_id: str, type_id: str) -> dict:
        """
        Get everything the QA module needs.
        Quality thresholds + voice (for tone checking) + article type specs.
        """
        site = self.load(site_id)
        article_type = self.get_article_type(site_id, type_id)

        return {
            "site_id": site.site_id,
            "site_name": site.site_name,
            "voice": site.voice,
            "quality": site.quality,
            "article_type": article_type,
            "products": site.products,
        }

    def get_output_config(self, site_id: str) -> dict:
        """Get output/deposit configuration for a site."""
        site = self.load(site_id)
        return {
            "site_id": site.site_id,
            "obsidian_folder": site.output.get("obsidian_folder", ""),
            "frontmatter_template": site.output.get("frontmatter_template", {}),
            "filename_pattern": site.output.get("filename_pattern", "YYYY-MM-DD-{slug}.md"),
            "status_field": site.output.get("status_field", "draft"),
        }

    def get_research_partners(self, site_id: str) -> list[str]:
        """Get list of site_ids that share research with this site."""
        site = self.load(site_id)
        return site.research.get("shared_with", [])

    # ── Discovery ───────────────────────────────────────────

    def list_sites(self) -> list[str]:
        """List all available site_ids based on config files found."""
        sites = []
        for f in self.config_dir.glob("*.yaml"):
            sites.append(f.stem)
        for f in self.config_dir.glob("*.yml"):
            sites.append(f.stem)
        return sorted(set(sites))

    def list_sites_by_tier(self, tier: str) -> list[str]:
        """List site_ids filtered by tier (flagship, daily, network)."""
        all_sites = self.load_all()
        return [sid for sid, ctx in all_sites.items() if ctx.tier == tier]

    # ── Validation ──────────────────────────────────────────

    def validate_all(self) -> dict:
        """
        Validate all site configs. Returns a report dict.
        Use on startup or as a health check.
        """
        report = {"valid": [], "invalid": [], "warnings": []}

        for site_id in self.list_sites():
            try:
                self.load(site_id, force_reload=True)
                report["valid"].append(site_id)
            except SiteConfigError as e:
                report["invalid"].append({"site_id": site_id, "error": str(e)})

        logger.info(
            f"Validation complete: {len(report['valid'])} valid, "
            f"{len(report['invalid'])} invalid"
        )
        return report

    def _validate(self, raw: dict, config_path: Path) -> None:
        """Validate a raw config dict against required fields."""
        errors = []

        # Top-level required fields
        for field_name in REQUIRED_TOP_LEVEL:
            if not raw.get(field_name):
                errors.append(f"Missing required field: {field_name}")

        # Tier validation
        tier = raw.get("tier", "")
        if tier and tier not in VALID_TIERS:
            errors.append(f"Invalid tier '{tier}'. Must be one of: {VALID_TIERS}")

        # Voice validation
        voice = raw.get("voice", {})
        if voice:
            for field_name in REQUIRED_VOICE:
                if not voice.get(field_name):
                    errors.append(f"Missing required voice field: {field_name}")

            pov = voice.get("pov", "")
            if pov and pov not in VALID_POV:
                errors.append(f"Invalid POV '{pov}'. Must be one of: {VALID_POV}")

        # Article types validation
        article_types = raw.get("article_types", [])
        if not article_types:
            errors.append("No article_types defined")
        else:
            type_ids = []
            for i, at in enumerate(article_types):
                for field_name in REQUIRED_ARTICLE_TYPE:
                    if field_name not in at:
                        errors.append(
                            f"Article type [{i}] missing: {field_name}"
                        )
                tid = at.get("type_id", "")
                if tid in type_ids:
                    errors.append(f"Duplicate article type_id: {tid}")
                type_ids.append(tid)

                # Validate enums
                rd = at.get("research_depth", "")
                if rd and rd not in VALID_RESEARCH_DEPTHS:
                    errors.append(
                        f"Article type '{tid}': invalid research_depth '{rd}'"
                    )

        # Quality thresholds sanity check
        quality = raw.get("quality", {})
        if quality:
            kill = quality.get("kill_threshold", 0)
            rewrite = quality.get("rewrite_threshold", 0)
            publish = quality.get("publish_threshold", 0)
            if not (kill <= rewrite <= publish):
                errors.append(
                    f"Quality thresholds out of order: "
                    f"kill({kill}) <= rewrite({rewrite}) <= publish({publish})"
                )

        if errors:
            raise SiteConfigError(
                f"Config validation failed for {config_path.name}:\n"
                + "\n".join(f"  - {e}" for e in errors)
            )

    # ── Internal Helpers ────────────────────────────────────

    def _find_config(self, site_id: str) -> Path:
        """Find config file for a site_id."""
        for ext in [".yaml", ".yml"]:
            path = self.config_dir / f"{site_id}{ext}"
            if path.exists():
                return path

        raise SiteNotFoundError(
            f"No config found for site '{site_id}'. "
            f"Looked in: {self.config_dir}\n"
            f"Available: {self.list_sites()}"
        )

    def _read_yaml(self, path: Path) -> dict:
        """Read and parse a YAML file."""
        try:
            with open(path, "r") as f:
                data = yaml.safe_load(f)
            if not isinstance(data, dict):
                raise SiteConfigError(f"Config is not a dict: {path}")
            return data
        except yaml.YAMLError as e:
            raise SiteConfigError(f"YAML parse error in {path}: {e}")

    def _build_context(self, raw: dict) -> SiteContext:
        """Build a SiteContext from a raw config dict."""
        return SiteContext(
            site_id=raw.get("site_id", ""),
            site_name=raw.get("site_name", ""),
            domain=raw.get("domain", ""),
            tier=raw.get("tier", ""),
            niche=raw.get("niche", ""),
            sub_niche=raw.get("sub_niche", ""),
            audience=raw.get("audience", {}),
            voice=raw.get("voice", {}),
            article_types=raw.get("article_types", []),
            categories=raw.get("categories", []),
            seo=raw.get("seo", {}),
            products=raw.get("products", {}),
            quality=raw.get("quality", {}),
            research=raw.get("research", {}),
            output=raw.get("output", {}),
            _raw=raw,
        )


# ── CLI Usage ───────────────────────────────────────────────
if __name__ == "__main__":
    """
    Run standalone to validate all configs:
        python site_loader.py
        python site_loader.py --site lamphill
    """
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    config_dir = os.environ.get("FACTORY_CONFIG_DIR", "config/sites")
    loader = SiteLoader(config_dir=config_dir)

    if len(sys.argv) > 2 and sys.argv[1] == "--site":
        # Validate single site
        site_id = sys.argv[2]
        try:
            site = loader.load(site_id)
            print(f"\n✅ {site.site_name} ({site.site_id})")
            print(f"   Tier: {site.tier}")
            print(f"   Niche: {site.niche}")
            print(f"   Article types: {site.get_article_type_ids()}")
            print(f"   Quality threshold: {site.quality.get('publish_threshold', 'N/A')}")
            print(f"   Research shared with: {site.research.get('shared_with', [])}")
        except SiteConfigError as e:
            print(f"\n❌ {e}")
            sys.exit(1)
    else:
        # Validate all
        print(f"\nValidating all configs in: {config_dir}\n")
        report = loader.validate_all()

        for sid in report["valid"]:
            site = loader.load(sid)
            print(f"  ✅ {sid} ({site.tier}) — {len(site.get_enabled_article_types())} article types")

        for item in report["invalid"]:
            print(f"  ❌ {item['site_id']}: {item['error']}")

        print(f"\n  Total: {len(report['valid'])} valid, {len(report['invalid'])} invalid")
