#!/usr/bin/env python3
"""
Fetch specific job URLs, build enriched JobRecords with Gemini + OpenAI CV
tailoring, write to Firestore, and save plain-text tailored CVs to ~/Downloads.
"""
from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

# ── Load .env before any other imports that read env vars ───────────────
_ENV_PATH = Path("/Users/adeomosanya/Documents/job apps/roles/.env")
if _ENV_PATH.exists():
    for line in _ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())

import requests
from bs4 import BeautifulSoup

# ── Import everything we need from daily_job_search ─────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from daily_job_search import (  # noqa: E402
    JobRecord,
    score_fit,
    build_reasons,
    build_gaps,
    build_preference_match,
    enhance_records_with_gemini,
    enhance_records_with_openai_cv,
    write_records_to_firestore,
    JOB_DIGEST_PROFILE_TEXT,
    USER_AGENT,
)

# ── Target URLs ─────────────────────────────────────────────────────────
TARGET_URLS = [
    "https://careers.lendable.com/jobs/7a180b51-d767-4621-8de3-23b9eb1c13b1-product-manager",
    "https://careers.airwallex.com/job/dc863c3d-b6b3-4dea-8dd0-5e404080a09c/senior-manager-operations-strategy/",
    "https://checkout.wd3.myworkdayjobs.com/CheckoutCareers/job/London/Product-Manager---Balances_R8638",
    "https://careers.fisglobal.com/us/en/job/JR0304593/Product-Manager-Commercial",
    "https://jobs.experian.com/job/product-manager-identity-and-fraud-in-london-england-jid-4056",
    "https://jobs.experian.com/job/product-manager-commercial-credit-and-risk-in-london-england-jid-4222",
]

DOWNLOADS_DIR = Path.home() / "Downloads"

# ── Helpers ─────────────────────────────────────────────────────────────

def _get(url: str) -> str:
    """Fetch a URL with retries, return HTML text."""
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            print(f"  Attempt {attempt + 1} failed for {url}: {exc}")
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    return ""


def _extract_meta(html: str, url: str) -> dict:
    """Extract title, company, location, description from a job page."""
    soup = BeautifulSoup(html, "html.parser")

    # --- Title ---
    title = ""
    for sel in [
        "h1",
        '[data-testid="job-title"]',
        ".job-title",
        ".posting-headline h2",
        '[class*="job-title"]',
        '[class*="JobTitle"]',
    ]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            title = el.get_text(strip=True)
            break
    if not title:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            title = og["content"]
    if not title:
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True).split("|")[0].split("–")[0].strip()

    # --- Company ---
    company = ""
    # Try extracting from URL hostname
    host = url.split("//")[-1].split("/")[0].lower()
    host_map = {
        "careers.lendable.com": "Lendable",
        "careers.airwallex.com": "Airwallex",
        "checkout.wd3.myworkdayjobs.com": "Checkout.com",
        "careers.fisglobal.com": "FIS Global",
        "jobs.experian.com": "Experian",
    }
    company = host_map.get(host, "")
    if not company:
        for sel in [
            '[class*="company"]',
            '[data-testid="company-name"]',
            ".company-name",
        ]:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                company = el.get_text(strip=True)
                break

    # --- Location ---
    location = ""
    for sel in [
        '[class*="location"]',
        '[data-testid="location"]',
        ".job-location",
        ".location",
    ]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            location = el.get_text(strip=True)
            break
    if not location:
        # Scan for London in page text
        text_all = soup.get_text(" ", strip=True).lower()
        if "london" in text_all:
            location = "London, UK"

    # --- Description ---
    desc = ""
    for sel in [
        '[class*="description"]',
        '[class*="job-details"]',
        '[class*="posting-"]',
        '[data-testid="job-description"]',
        "article",
        ".job-description",
        "#job-description",
    ]:
        el = soup.select_one(sel)
        if el:
            desc = el.get_text(" ", strip=True)
            if len(desc) > 200:
                break
    if len(desc) < 200:
        # Fallback: get the full body text
        desc = soup.get_text(" ", strip=True)

    # Truncate long descriptions to keep prompts manageable
    if len(desc) > 8000:
        desc = desc[:8000]

    return {
        "title": title or "Product Manager",
        "company": company or "Unknown",
        "location": location or "London, UK",
        "description": desc,
    }


def getTailoredCvPlainText(record: JobRecord) -> str:
    """Generate the same plain-text CV format used in the portal."""
    sections = record.tailored_cv_sections or {}

    # Base CV sections (mirrors app.cv.js BASE_CV_SECTIONS)
    base_summary = (
        "13+ years in financial crime product and operations management, delivering onboarding "
        "and screening platform transformations across EMEA, AMER and APAC. Independently created "
        "and deployed three live RegTech products used by compliance officers and regulated firms. "
        "Led enterprise KYC and screening platform strategy and configuration (Fenergo, Napier, Enate) "
        "for thousands of business clients globally, designing scalable 1st line controls and operating "
        "models. Proven stakeholder manager from front office to C-suite."
    )
    base_achievements = [
        "55% reduction in client onboarding time (45 days → 20 days) across EMEA, AMER and APAC",
        "20% operational headcount efficiency through workflow automation",
        "18+ reporting dashboards deployed, used by hundreds of users across APAC and EMEA",
        "3 live RegTech products independently created and deployed using AI-assisted development",
        "Napier screening implementation was subsequently validated by Dutch DNB effectiveness assessment",
        "12 BaFin audit points closed, mitigating multimillion-pound fine exposure",
        "£120k ARR secured from Tier 1 global bank proof of concept",
    ]
    base_vistra = [
        "Led 1st line design and implementation of business onboarding and financial crime controls for corporate and fund clients, using Fenergo (KYC), Napier (screening) and Enate (orchestration) across EMEA, AMER and APAC",
        "Defined platform feature requirements and competitor positioning; secured £400k+ business case sign-off",
        "Led vendor evaluation and pricing negotiation, balancing regulatory and commercial constraints",
        "Defined Fenergo KYC product model across EMEA, AMER and APAC — global consistency with jurisdiction-specific CDD/EDD logic",
        "Owned Napier screening design and capacity framework; validated by Dutch DNB effectiveness assessment",
        "Created Enate orchestration layer from fragmented processes — 55% faster onboarding (45 → 20 days), thousands of clients annually",
        "Gathered requirements and built Power BI suite (screening, KYC, onboarding dashboards) through direct discovery with APAC, AMER and EMEA teams",
        "Managed 4 Business Analysts (reporting, data migration, SOPs, tech implementation); coordinated delivery across engineering, compliance, and front office",
        "Chaired SteerCo with CFO/COO; delivered QA academy for 150+ analysts across 20 countries",
    ]
    base_ebury = [
        "Built onboarding funnel analytics, identifying drop-off points; drove 20% conversion uplift across Spain, Greece and Germany",
        "Optimised screening thresholds - 38% false positive reduction, regulatory standards maintained",
        "Led Salesforce to Fenergo migration (50k+ client records): data quality strategy, vendor management, zero-downtime cutover",
        "Designed continuous monitoring for medium/low-risk segments — 60% reduction in client review touchpoints",
    ]

    def bullet(text):
        cleaned = re.sub(r'^[-•\s]*', '', text).rstrip('.')
        return f"• {cleaned}"

    lines = []

    # Header
    lines.append("ADE OMOSANYA")
    lines.append("London, UK | 07920497486 | ademolaomosanya@gmail.com")
    lines.append("LinkedIn | Portfolio: FCA Fines Dashboard | Vulnerability Portal | SMCR Platform\n")

    # Professional Summary
    lines.append("PROFESSIONAL SUMMARY")
    lines.append((sections.get("summary") or base_summary) + "\n")

    # Key Achievements
    lines.append("KEY ACHIEVEMENTS")
    for b in (sections.get("key_achievements") or base_achievements):
        lines.append(bullet(b))

    # Professional Experience
    lines.append("\nPROFESSIONAL EXPERIENCE")

    # Vistra — flat bullets, no sub-headings
    lines.append("\nVISTRA | Global Corporate Services (9,000+ employees, $1.5B revenue)")
    lines.append("Global Product & Process Owner – Onboarding, KYC & Screening | September 2023 – Present")
    vistra_bullets = sections.get("vistra_bullets") or base_vistra
    for b in vistra_bullets:
        lines.append(bullet(b))

    # Ebury
    lines.append("\nEBURY | B2B Foreign Exchange Platform (Series E, £1.7B valuation)")
    lines.append("Product Manager – Identity & Financial Crime | April 2022 – September 2023")
    for b in (sections.get("ebury_bullets") or base_ebury):
        lines.append(bullet(b))

    # MEMA Consultants
    lines.append("\nMEMA CONSULTANTS | RegTech & Compliance Solutions")
    lines.append("Founder & Director | March 2017 – Present")
    lines.append(bullet("Built and deployed 3 live RegTech products (Next.js/React, AI-assisted) used by compliance officers and regulated SMEs:"))
    lines.append("  FCA Fines Dashboard | Regulatory enforcement analytics")
    lines.append("  Vulnerability Portal | Consumer Duty compliance")
    lines.append("  SMCR Platform | Senior Managers regime mapping")
    lines.append(bullet("Advisory: FCA authorisation, financial crime framework design, horizon scanning tooling"))

    # Elucidate
    lines.append("\nELUCIDATE | RegTech SaaS Platform")
    lines.append("Product Manager | September 2020 – March 2022")
    lines.append(bullet("Zero-to-one: discovery, solution design, PoC delivery — Tier 1 bank, £120k ARR"))
    lines.append(bullet("Post-PoC: built networking feature from customer discovery; 8 firms onboarded"))
    lines.append(bullet("Redesigned platform UX — 40% MAU uplift, deployment reduced to 6 weeks"))

    # N26
    lines.append("\nN26 | Digital Banking (7M+ customers)")
    lines.append("Financial Crime Product Lead | September 2019 – September 2020")
    lines.append(bullet("Led remediation programme addressing BaFin regulatory concerns; defined product requirements across transaction monitoring, screening, and enhanced due diligence"))
    lines.append(bullet("Established EDD squad; cleared 470 PEP backlog and automated 70% of review processes"))

    # Previous Experience
    lines.append("\nPrevious Experience")
    lines.append(bullet("ERNST & YOUNG – Senior Associate, Financial Crime Advisory (2017–2019)"))
    lines.append(bullet("MAZARS – Assistant Manager, Financial Services Consulting (2015–2017)"))
    lines.append(bullet("FINANCIAL CONDUCT AUTHORITY – Associate, Authorisations (2014–2015)"))
    lines.append(bullet("FINANCIAL OMBUDSMAN SERVICE – Investment Adjudicator (2012–2014)"))

    # Technical & Product Capabilities
    lines.append("\nTECHNICAL & PRODUCT CAPABILITIES")
    lines.append("• Platforms- KYC: Fenergo | Sanctions & Screening: Napier, LexisNexis Bridger | Onboarding Orchestration: Enate | ID&V: Jumio, Onfido, IDnow | CRM: Salesforce")
    lines.append("• Technical- SQL, PostgreSQL | Power BI, Excel | Next.js/React, Vercel, Netlify, GitHub | API integration | Jira, Confluence | Figma, Miro")
    lines.append("• Product- Zero-to-one builds, discovery, roadmap ownership, business cases, competitor analysis, capacity planning, Kanban")
    lines.append("• Regulatory- UK FCA/MLR/JMLSG, EU- Dutch DNB, BaFin, Hong Kong SFC, Singapore MAS, OFAC/OFSI, EU AMLD")

    # Education
    lines.append("\nUniversity of Hull – LLB Law (2007–2010)")
    lines.append("ACAMS Certified (2018) | ICA Fellow (2020) | APCC Member")

    return "\n".join(lines)


# ── Main ────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Processing {len(TARGET_URLS)} target job URLs…\n")

    records: list[JobRecord] = []

    for url in TARGET_URLS:
        print(f"→ Fetching: {url}")
        html = _get(url)
        if not html:
            print(f"  ✗ Could not fetch {url}\n")
            continue

        meta = _extract_meta(html, url)
        title = meta["title"]
        company = meta["company"]
        location = meta["location"]
        description = meta["description"]

        print(f"  Title: {title}")
        print(f"  Company: {company}")
        print(f"  Location: {location}")
        print(f"  Description length: {len(description)} chars")

        # Score fit (includes ATS keyword matching)
        fit_score, domain_terms, extra_terms, ats = score_fit(description, company)
        why_fit = build_reasons(description)
        cv_gap = build_gaps(description)
        pref_match = build_preference_match(description, company, location)

        record = JobRecord(
            role=title,
            company=company,
            location=location,
            link=url,
            posted=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            source="direct-url",
            fit_score=fit_score,
            preference_match=pref_match,
            why_fit=why_fit,
            cv_gap=cv_gap,
            notes=description[:4000],
            ats_keywords_found=ats.get("ats_keywords_found", []),
            ats_keywords_missing=ats.get("ats_keywords_missing", []),
            ats_keyword_coverage=ats.get("ats_keyword_coverage", 0),
        )
        records.append(record)
        print(f"  Fit: {fit_score}% | ATS coverage: {record.ats_keyword_coverage}%\n")

    if not records:
        print("No records to process.")
        return

    # Step 1: Enhance with Gemini (tailored bullets, prep Q&A, STAR stories, etc.)
    print("─── Enhancing with Gemini ───")
    records = enhance_records_with_gemini(records)
    for r in records:
        has_summary = bool(r.tailored_summary)
        has_bullets = bool(r.tailored_cv_bullets)
        print(f"  {r.company}: tailored_summary={'yes' if has_summary else 'NO'}, bullets={'yes' if has_bullets else 'NO'}")

    # Step 2: Enhance with OpenAI (tailored CV sections)
    print("\n─── Generating tailored CV sections with OpenAI ───")
    records = enhance_records_with_openai_cv(records)
    for r in records:
        has_cv = bool(r.tailored_cv_sections)
        keys = list(r.tailored_cv_sections.keys()) if r.tailored_cv_sections else []
        print(f"  {r.company}: cv_sections={'yes' if has_cv else 'NO'} keys={keys}")

    # Step 3: Write to Firestore
    print("\n─── Writing to Firestore ───")
    write_records_to_firestore(records)
    print(f"  ✓ {len(records)} records written to Firestore")

    # Step 4: Write tailored CV text files to ~/Downloads
    print(f"\n─── Saving tailored CVs to {DOWNLOADS_DIR} ───")
    for record in records:
        company_slug = re.sub(r"[^a-zA-Z0-9]", "", record.company)
        role_slug = re.sub(r"[^a-zA-Z0-9]", "", record.role)[:30]
        filename = f"CV_Tailored_{company_slug}_{role_slug}.txt"
        filepath = DOWNLOADS_DIR / filename
        cv_text = getTailoredCvPlainText(record)
        filepath.write_text(cv_text, encoding="utf-8")
        print(f"  ✓ {filename} ({len(cv_text)} chars)")

    print(f"\nDone. {len(records)} roles processed.")


if __name__ == "__main__":
    main()
