#!/usr/bin/env python3
"""
Site Onboarding CLI — Entry Point

Onboards a new domain end-to-end:
  1. Vercel domain registration
  2. Namecheap DNS configuration
  3. Google Search Console verification
  4. Site YAML creation

Usage:
    python onboard_site.py --domain=example.com --dry-run
    python onboard_site.py --domain=example.com
    python onboard_site.py --domain=example.com --skip-vercel
    python onboard_site.py --domain=example.com --verbose

See ONBOARDING_SPEC.md for full documentation.
"""
import sys

from onboarding.cli import main

if __name__ == "__main__":
    sys.exit(main())
