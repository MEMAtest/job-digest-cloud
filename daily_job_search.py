#!/usr/bin/env python3
"""
Daily job search and email digest for KYC/AML/onboarding product roles.
Sources: LinkedIn guest endpoints, Greenhouse boards (optional).
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:  # noqa: BLE001
    ZoneInfo = None

import pandas as pd
import requests
from bs4 import BeautifulSoup


@dataclass
class JobRecord:
    role: str
    company: str
    location: str
    link: str
    posted: str
    source: str
    fit_score: int
    preference_match: str
    why_fit: str
    cv_gap: str
    notes: str


DEFAULT_BASE_DIR = Path("/Users/adeomosanya/Documents/job apps/roles")
BASE_DIR = Path(os.getenv("JOB_DIGEST_BASE_DIR", str(DEFAULT_BASE_DIR)))
DIGEST_DIR = BASE_DIR / "digests"
DIGEST_DIR.mkdir(parents=True, exist_ok=True)

TZ_NAME = os.getenv("JOB_DIGEST_TZ", "Europe/London")
WINDOW_HOURS = int(os.getenv("JOB_DIGEST_WINDOW_HOURS", "24"))
MIN_SCORE = int(os.getenv("JOB_DIGEST_MIN_SCORE", "70"))
MAX_EMAIL_ROLES = int(os.getenv("JOB_DIGEST_MAX_EMAIL_ROLES", "12"))
PREFERENCES = os.getenv(
    "JOB_DIGEST_PREFERENCES",
    "London or remote UK · Product/Platform roles · KYC/AML/Onboarding/Sanctions/Screening · Min fit 70%",
)
SOURCES_SUMMARY = os.getenv(
    "JOB_DIGEST_SOURCES",
    "LinkedIn (guest search + company search) · Greenhouse boards · Lever boards · SmartRecruiters",
)
SEEN_CACHE_PATH = Path(
    os.getenv("JOB_DIGEST_SEEN_CACHE", str(DIGEST_DIR / "sent_links.json"))
)
SEEN_CACHE_DAYS = int(os.getenv("JOB_DIGEST_SEEN_CACHE_DAYS", "14"))
RUN_AT = os.getenv("JOB_DIGEST_RUN_AT", "")
RUN_WINDOW_MINUTES = int(os.getenv("JOB_DIGEST_RUN_WINDOW_MINUTES", "20"))
RUN_STATE_PATH = Path(
    os.getenv("JOB_DIGEST_RUN_STATE", str(DIGEST_DIR / "run_state.json"))
)

EMAIL_ENABLED = os.getenv("JOB_DIGEST_EMAIL_ENABLED", "true").lower() == "true"
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
FROM_EMAIL = os.getenv("FROM_EMAIL", "")
TO_EMAIL = os.getenv("TO_EMAIL", "ademolaomosanya@gmail.com")

USER_AGENT = os.getenv(
    "JOB_DIGEST_USER_AGENT",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
)

SEARCH_KEYWORDS = [
    "product manager kyc",
    "product manager aml",
    "product manager onboarding",
    "product manager screening",
    "product manager financial crime",
    "product manager compliance",
    "product manager identity",
    "product manager fraud",
    "product manager regtech",
    "product manager kyb",
    "product manager cdd",
    "product manager edd",
    "product owner kyc",
    "product owner onboarding",
    "product owner compliance",
    "product owner screening",
    "product manager transaction monitoring",
    "product manager client onboarding",
    "product manager customer onboarding",
    "product manager account opening",
    "product manager client lifecycle",
    "product manager customer lifecycle",
    "product manager clm",
    "product lead risk",
    "product lead compliance",
    "product manager sanctions",
    "product manager due diligence",
    "product manager case management",
    "product manager investigation",
    "product manager fraud prevention",
    "product manager identity verification",
    "product manager onboarding platform",
    "product manager compliance platform",
    "product manager risk platform",
    "product manager screening platform",
    "product manager financial crime platform",
]

COMPANY_SEARCH_TERMS = [
    "product manager",
    "product owner",
    "product lead",
    "product manager onboarding",
    "product manager compliance",
    "product manager risk",
    "product manager fraud",
    "product management",
    "product operations",
    "product specialist",
]

SEARCH_COMPANIES = [
    # Banks & FS
    "Barclays",
    "HSBC",
    "NatWest",
    "Lloyds",
    "Lloyds Banking Group",
    "Santander",
    "Nationwide",
    "TSB",
    "Virgin Money",
    "Metro Bank",
    "Tesco Bank",
    "Coutts",
    "Standard Chartered",
    "Citi",
    "JPMorgan",
    "Goldman Sachs",
    "Morgan Stanley",
    "Bank of America",
    "Deutsche Bank",
    "UBS",
    "BNP Paribas",
    "RBC",
    "ING",
    "Rabobank",
    "ABN AMRO",
    "UniCredit",
    "LSEG",
    # Fintech / Payments
    "Wise",
    "Revolut",
    "Monzo",
    "Starling",
    "Engine by Starling",
    "Tide",
    "Zopa",
    "OakNorth",
    "Atom Bank",
    "ClearBank",
    "Kroo",
    "Chip",
    "Curve",
    "Checkout.com",
    "Stripe",
    "Adyen",
    "Worldpay",
    "GoCardless",
    "Modulr",
    "TrueLayer",
    "Tink",
    "Plaid",
    "Airwallex",
    "Rapyd",
    "Marqeta",
    "Mambu",
    "Thought Machine",
    "Temenos",
    "Avaloq",
    "Funding Circle",
    "Lendable",
    "Zilch",
    "Solaris",
    # RegTech / KYC / Screening
    "Fenergo",
    "Quantexa",
    "ComplyAdvantage",
    "LexisNexis Risk",
    "NICE Actimize",
    "Actimize",
    "Pega",
    "FIS",
    "Moody's",
    "S&P Global",
    "Oracle",
    "Appian",
    "Dow Jones",
    "Napier",
    "Trulioo",
    "Onfido",
    "Sumsub",
    "Veriff",
    "Feedzai",
    "Socure",
    "Kyckr",
    "KYC360",
    "Ripjar",
    "FinScan",
    "IMTF",
    "Saphyre",
    "SymphonyAI",
    "Alloy",
    "Incode",
    "Norbloc",
    "smartKYC",
    "KYC Portal",
    "Encompass",
    "Sigma360",
    "Davies",
    # Big Tech
    "Google",
    "Microsoft",
    "Amazon",
    "Apple",
    "Meta",
    "Salesforce",
    "Oracle",
    "SAP",
]

SEARCH_LOCATIONS = [
    "London, United Kingdom",
    "United Kingdom",
    "Remote",
]

EXCLUDE_TITLE_TERMS = {"growth"}
EXCLUDE_COMPANIES = {"ebury"}

ROLE_TITLE_REQUIREMENTS = {
    "manager",
    "owner",
    "lead",
    "principal",
    "head",
    "director",
    "specialist",
    "strategy",
    "operations",
    "management",
    "vp",
}

VENDOR_COMPANIES = {
    "fenergo",
    "complyadvantage",
    "quantexa",
    "lexisnexis",
    "nice actimize",
    "actimize",
    "pega",
    "oracle",
    "fis",
    "moody",
    "s&p global",
    "appian",
    "kyc360",
    "ripjar",
    "symphonyai",
    "saphyre",
    "encompass",
    "napier",
    "bridger",
    "dow jones",
    "alloy",
    "onfido",
    "trulioo",
    "sumsub",
    "veriff",
    "socure",
    "experian",
    "kyckr",
    "entrust",
    "finscan",
    "imtf",
    "norbloc",
    "smartkyc",
    "kyc portal",
}

FINTECH_COMPANIES = {
    "wise",
    "airwallex",
    "revolut",
    "monzo",
    "starling",
    "engine by starling",
    "visa",
    "mastercard",
    "worldpay",
    "checkout.com",
    "stripe",
    "modulr",
    "gocardless",
    "klarna",
    "n26",
    "tide",
    "mollie",
    "jpmorganchase",
    "goldman sachs",
    "marcus",
    "lseg",
    "broadridge",
    "davies",
    "experian",
    "socure",
    "kyckr",
    "quantexa",
    "complyadvantage",
    "plaid",
    "truelayer",
    "tink",
    "marqeta",
    "adyen",
    "rapyd",
    "curve",
    "chip",
    "kroo",
    "zopa",
    "oaknorth",
    "clearpay",
    "funding circle",
    "lendable",
    "zilch",
}

BANK_COMPANIES = {
    "barclays",
    "hsbc",
    "natwest",
    "lloyds",
    "lloyds banking group",
    "santander",
    "standard chartered",
    "citi",
    "jpmorgan",
    "goldman sachs",
    "morgan stanley",
    "bank of america",
    "deutsche bank",
    "ubs",
    "lseg",
    "nationwide",
    "tsb",
    "virgin money",
    "metro bank",
    "tesco bank",
    "coutts",
    "bnp paribas",
    "rbc",
    "ing",
    "rabobank",
    "abn amro",
    "unicredit",
}

TECH_COMPANIES = {
    "google",
    "microsoft",
    "amazon",
    "apple",
    "meta",
    "salesforce",
    "oracle",
    "sap",
    "servicenow",
    "atlassian",
}

DOMAIN_TERMS = [
    "kyc",
    "aml",
    "onboarding",
    "screening",
    "financial crime",
    "transaction monitoring",
    "sanctions",
    "identity",
    "fraud",
    "compliance",
    "due diligence",
    "edd",
    "cdd",
    "kyb",
    "clm",
    "client lifecycle",
    "customer lifecycle",
    "account opening",
    "account onboarding",
    "client onboarding",
    "regulatory",
    "regtech",
    "case management",
    "investigation",
]

EXTRA_TERMS = [
    "api",
    "platform",
    "data",
    "analytics",
    "dashboard",
    "workflow",
    "orchestration",
    "decisioning",
    "rules",
    "configuration",
    "integration",
]

GAP_TERMS = {
    "lending": "Highlight any lending or credit lifecycle exposure.",
    "credit": "Highlight any credit decisioning or lending exposure.",
    "mobile": "Show any mobile UX or app product experience.",
    "consumer": "Emphasize consumer or retail onboarding if applicable.",
    "payments": "Show any payments or merchant onboarding experience.",
    "merchant": "Add any merchant onboarding or acquiring examples.",
    "ml": "Call out ML or model-driven risk tooling if relevant.",
    "machine learning": "Call out ML or model-driven risk tooling if relevant.",
    "data platform": "Emphasize data platform and data quality ownership.",
}

REASON_HINTS = {
    "onboarding": "Onboarding workflow ownership fits your KYC/onboarding platform delivery.",
    "kyc": "KYC domain aligns with your screening and compliance controls work.",
    "aml": "AML product experience aligns with your financial crime delivery.",
    "fraud": "Fraud prevention aligns with your screening-threshold optimization work.",
    "identity": "Identity verification aligns with your onboarding and risk controls background.",
    "case management": "Case management aligns with investigation and alert triage workflows.",
    "investigation": "Investigation workflow ownership aligns with your financial crime delivery.",
    "data": "Data and analytics product work aligns with your reporting dashboard builds.",
    "api": "Platform/API focus matches your integration and orchestration experience.",
    "clm": "Client lifecycle management aligns with your onboarding and screening background.",
    "client lifecycle": "Client lifecycle management aligns with your onboarding and screening background.",
    "customer lifecycle": "Customer lifecycle management aligns with your onboarding and screening background.",
    "account opening": "Account opening aligns with onboarding and journey design experience.",
    "kyb": "KYB exposure aligns with your complex entity onboarding experience.",
    "screening": "Screening and monitoring align with your financial crime controls work.",
}

GREENHOUSE_BOARDS = [
    "complyadvantage",
    "appian",
    "socure",
    "symphonyai",
    "entrust",
    "quantexa",
    "kyckr",
    "kyc360",
    "ripjar",
    "fenergo",
    "veriff",
    "onfido",
    "trulioo",
    "sumsub",
    "napier",
    "plaid",
    "marqeta",
    "checkoutcom",
    "gocardless",
    "truelayer",
    "tink",
    "mollie",
    "klarna",
    "airwallex",
    "modulr",
    "mambu",
    "zopa",
    "thought-machine",
]

LEVER_BOARDS = [
    "onfido",
    "trulioo",
    "sumsub",
    "veriff",
    "kyckr",
    "clearscore",
    "tide",
    "monzo",
    "airwallex",
    "revolut",
    "checkout",
    "gocardless",
    "wise",
    "truelayer",
    "modulr",
    "curve",
    "chip",
    "kroo",
    "zopa",
    "oaknorth",
]

SMARTRECRUITERS_COMPANIES = [
    "Visa",
]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def clean_link(url: str) -> str:
    return url.split("?")[0] if url else url


def load_seen_cache(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {k: v for k, v in data.items() if isinstance(k, str) and isinstance(v, str)}


def prune_seen_cache(seen: Dict[str, str], max_age_days: int) -> Dict[str, str]:
    cutoff = now_utc() - timedelta(days=max_age_days)
    pruned: Dict[str, str] = {}
    for link, ts in seen.items():
        try:
            dt = datetime.fromisoformat(ts)
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        if dt >= cutoff:
            pruned[link] = dt.isoformat()
    return pruned


def save_seen_cache(path: Path, seen: Dict[str, str]) -> None:
    try:
        path.write_text(json.dumps(seen, indent=2))
    except OSError:
        pass


def filter_new_records(records: List[JobRecord], seen: Dict[str, str]) -> List[JobRecord]:
    new_records = []
    for record in records:
        if not record.link:
            new_records.append(record)
            continue
        if record.link in seen:
            continue
        new_records.append(record)
    return new_records


def select_top_pick(records: List[JobRecord]) -> Optional[JobRecord]:
    if not records:
        return None
    return max(records, key=lambda record: record.fit_score)


def load_run_state(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {k: v for k, v in data.items() if isinstance(k, str) and isinstance(v, str)}


def save_run_state(path: Path, state: Dict[str, str]) -> None:
    try:
        path.write_text(json.dumps(state, indent=2))
    except OSError:
        pass


def should_run_now() -> bool:
    if not RUN_AT:
        return True
    if ZoneInfo is None:
        return True

    try:
        hour_str, minute_str = RUN_AT.split(":")
        target_hour = int(hour_str)
        target_minute = int(minute_str)
    except ValueError:
        return True

    tz = ZoneInfo(TZ_NAME)
    now_local = datetime.now(tz)
    target = now_local.replace(
        hour=target_hour,
        minute=target_minute,
        second=0,
        microsecond=0,
    )

    delta_minutes = abs((now_local - target).total_seconds()) / 60.0
    if delta_minutes > RUN_WINDOW_MINUTES:
        return False

    state = load_run_state(RUN_STATE_PATH)
    last_run = state.get("last_run_date")
    if last_run == now_local.strftime("%Y-%m-%d"):
        return False

    return True


def parse_posted_within_window(posted_text: str, posted_date: str, window_hours: int) -> bool:
    text = (posted_text or "").lower().strip()
    if "just now" in text or "today" in text:
        return True
    if "yesterday" in text:
        return window_hours >= 24
    match = re.search(r"(\d+)", text)
    number = int(match.group(1)) if match else None

    if "minute" in text or "min" in text:
        return True
    if "hour" in text and number is not None:
        return number <= window_hours
    if "day" in text and number is not None:
        return (number * 24) <= window_hours
    if "week" in text and number is not None:
        return (number * 7 * 24) <= window_hours

    if posted_date:
        try:
            dt = datetime.fromisoformat(posted_date)
        except ValueError:
            return False
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return (now_utc() - dt) <= timedelta(hours=window_hours)

    return False


def score_fit(text: str, company: str) -> Tuple[int, List[str], List[str]]:
    text_l = text.lower()
    matched_domain = [t for t in DOMAIN_TERMS if t in text_l]
    matched_extra = [t for t in EXTRA_TERMS if t in text_l]

    score = 60
    if matched_domain:
        score += min(20, 4 * len(matched_domain))
    if matched_extra:
        score += min(10, 2 * len(matched_extra))
    company_l = company.lower()
    if any(v in company_l for v in VENDOR_COMPANIES):
        score += 12
    if any(f in company_l for f in FINTECH_COMPANIES):
        score += 8
    if any(b in company_l for b in BANK_COMPANIES):
        score += 6
    if any(t in company_l for t in TECH_COMPANIES):
        score += 4
    if "onboarding" in text_l or "kyc" in text_l:
        score += 3
    if "api" in text_l:
        score += 3

    return min(score, 90), matched_domain, matched_extra


def build_reasons(text: str) -> str:
    text_l = text.lower()
    reasons = []
    for key, reason in REASON_HINTS.items():
        if key in text_l:
            reasons.append(reason)
    if not reasons:
        reasons.append("Strong fit with your financial crime, onboarding, and platform delivery background.")
    return " ".join(reasons[:3])


def build_gaps(text: str) -> str:
    text_l = text.lower()
    gaps = []
    for key, hint in GAP_TERMS.items():
        if key in text_l:
            gaps.append(hint)
    if not gaps:
        gaps.append("No obvious gaps; emphasize cross-functional delivery and regulated environment experience.")
    return " ".join(gaps[:2])


def build_preference_match(text: str, company: str, location: str) -> str:
    text_l = text.lower()
    company_l = company.lower()
    location_l = location.lower()

    parts = []
    if any(term in location_l for term in ["london", "remote", "united kingdom", "hybrid"]):
        parts.append("London/Remote UK")
    if "product" in text_l:
        parts.append("Product role")
    if any(term in text_l for term in ["kyc", "aml", "screening", "onboarding", "financial crime", "sanctions"]):
        parts.append("KYC/AML/Onboarding")
    if any(vendor in company_l for vendor in VENDOR_COMPANIES):
        parts.append("RegTech/Vendor")
    if any(fintech in company_l for fintech in FINTECH_COMPANIES):
        parts.append("Fintech/Payments")
    if any(bank in company_l for bank in BANK_COMPANIES):
        parts.append("Bank/FS")
    if any(tech in company_l for tech in TECH_COMPANIES):
        parts.append("Big Tech")
    if "api" in text_l or "platform" in text_l:
        parts.append("Platform/API")

    return " · ".join(parts) if parts else "General product fit"


def is_relevant_title(title: str) -> bool:
    title_l = title.lower()
    if "product" not in title_l:
        return False
    if any(term in title_l for term in EXCLUDE_TITLE_TERMS):
        return False
    if not any(req in title_l for req in ROLE_TITLE_REQUIREMENTS):
        return False
    return True


def is_relevant_location(location: str) -> bool:
    loc_l = location.lower()
    return any(term in loc_l for term in ["london", "united kingdom", "remote", "hybrid"])


def linkedin_search(session: requests.Session) -> List[Dict[str, str]]:
    base_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    headers = {"User-Agent": USER_AGENT}
    jobs: Dict[str, Dict[str, str]] = {}

    for keywords in SEARCH_KEYWORDS:
        for location in SEARCH_LOCATIONS:
            for start in [0, 25]:
                params = {
                    "keywords": keywords,
                    "location": location,
                    "f_TPR": "r604800",
                    "start": start,
                }
                try:
                    resp = session.get(base_url, params=params, headers=headers, timeout=20)
                except requests.RequestException:
                    continue
                if resp.status_code != 200:
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")
                for card in soup.select("div.base-search-card"):
                    job_urn = card.get("data-entity-urn", "")
                    job_id = job_urn.split(":")[-1]
                    if not job_id:
                        continue

                    title_el = card.select_one("h3.base-search-card__title")
                    company_el = card.select_one("h4.base-search-card__subtitle")
                    location_el = card.select_one("span.job-search-card__location")
                    time_el = card.select_one("time")
                    link_el = card.select_one("a.base-card__full-link")

                    title = normalize_text(title_el.get_text()) if title_el else ""
                    company = normalize_text(company_el.get_text()) if company_el else ""
                    location_text = normalize_text(location_el.get_text()) if location_el else ""
                    posted_text = normalize_text(time_el.get_text()) if time_el else ""
                    posted_date = time_el.get("datetime") if time_el else ""
                    link = link_el.get("href") if link_el else ""

                    if not title or not company:
                        continue
                    if company.lower() in EXCLUDE_COMPANIES:
                        continue

                    jobs[job_id] = {
                        "job_id": job_id,
                        "title": title,
                        "company": company,
                        "location": location_text,
                        "posted_text": posted_text,
                        "posted_date": posted_date,
                        "link": clean_link(link),
                    }

                time.sleep(0.3)

    # Company-focused searches (narrower paging to reduce load)
    for company in SEARCH_COMPANIES:
        for base_term in COMPANY_SEARCH_TERMS:
            keywords = f"{base_term} {company}"
            for location in SEARCH_LOCATIONS:
                for start in [0]:
                    params = {
                        "keywords": keywords,
                        "location": location,
                        "f_TPR": "r604800",
                        "start": start,
                    }
                    try:
                        resp = session.get(base_url, params=params, headers=headers, timeout=20)
                    except requests.RequestException:
                        continue
                    if resp.status_code != 200:
                        continue

                    soup = BeautifulSoup(resp.text, "html.parser")
                    for card in soup.select("div.base-search-card"):
                        job_urn = card.get("data-entity-urn", "")
                        job_id = job_urn.split(":")[-1]
                        if not job_id:
                            continue

                        title_el = card.select_one("h3.base-search-card__title")
                        company_el = card.select_one("h4.base-search-card__subtitle")
                        location_el = card.select_one("span.job-search-card__location")
                        time_el = card.select_one("time")
                        link_el = card.select_one("a.base-card__full-link")

                        title = normalize_text(title_el.get_text()) if title_el else ""
                        company_name = normalize_text(company_el.get_text()) if company_el else ""
                        location_text = normalize_text(location_el.get_text()) if location_el else ""
                        posted_text = normalize_text(time_el.get_text()) if time_el else ""
                        posted_date = time_el.get("datetime") if time_el else ""
                        link = link_el.get("href") if link_el else ""

                        if not title or not company_name:
                            continue
                        if company_name.lower() in EXCLUDE_COMPANIES:
                            continue

                        jobs[job_id] = {
                            "job_id": job_id,
                            "title": title,
                            "company": company_name,
                            "location": location_text,
                            "posted_text": posted_text,
                            "posted_date": posted_date,
                            "link": clean_link(link),
                        }

                    time.sleep(0.2)

    return list(jobs.values())


def linkedin_job_details(session: requests.Session, job_id: str) -> Tuple[str, str, str]:
    detail_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = session.get(detail_url, headers=headers, timeout=20)
    except requests.RequestException:
        return "", "", ""
    if resp.status_code != 200:
        return "", "", ""

    soup = BeautifulSoup(resp.text, "html.parser")
    desc_el = soup.select_one("div.show-more-less-html__markup")
    desc_text = normalize_text(desc_el.get_text(" ")) if desc_el else ""

    posted_el = soup.select_one("span.posted-time-ago__text")
    posted_text = normalize_text(posted_el.get_text()) if posted_el else ""

    loc_el = soup.select_one("span.topcard__flavor--bullet")
    location_text = normalize_text(loc_el.get_text()) if loc_el else ""

    return desc_text, posted_text, location_text


def greenhouse_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    for board in GREENHOUSE_BOARDS:
        url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs"
        try:
            resp = session.get(url, timeout=20)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        data = resp.json()
        for job in data.get("jobs", []):
            title = job.get("title", "")
            if not title:
                continue
            company = board.replace("-", " ").title()
            location = (job.get("location") or {}).get("name", "")
            link = job.get("absolute_url", "")
            updated_at = job.get("updated_at", "")
            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "link": link,
                    "posted_text": "",
                    "posted_date": updated_at,
                }
            )
    return jobs


def lever_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    for board in LEVER_BOARDS:
        url = f"https://api.lever.co/v0/postings/{board}?mode=json"
        try:
            resp = session.get(url, timeout=20)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        if not isinstance(data, list):
            continue
        for job in data:
            title = job.get("text", "") or job.get("title", "")
            if not title:
                continue
            company = board.replace("-", " ").title()
            location = ""
            if isinstance(job.get("categories"), dict):
                location = job["categories"].get("location", "") or ""
            link = job.get("hostedUrl") or job.get("applyUrl") or ""
            posted_ms = job.get("createdAt")
            posted_date = ""
            if posted_ms:
                try:
                    posted_date = datetime.fromtimestamp(posted_ms / 1000, tz=timezone.utc).isoformat()
                except (OSError, ValueError):
                    posted_date = ""
            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "link": link,
                    "posted_text": "",
                    "posted_date": posted_date,
                }
            )
    return jobs


def smartrecruiters_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    for company in SMARTRECRUITERS_COMPANIES:
        offset = 0
        limit = 100
        while True:
            url = f"https://api.smartrecruiters.com/v1/companies/{company}/postings"
            params = {"limit": limit, "offset": offset, "q": "product"}
            try:
                resp = session.get(url, params=params, timeout=20)
            except requests.RequestException:
                break
            if resp.status_code != 200:
                break
            try:
                data = resp.json()
            except ValueError:
                break
            content = data.get("content", [])
            if not content:
                break

            for job in content:
                title = job.get("name", "")
                if not title:
                    continue
                company_name = (job.get("company") or {}).get("name", "") or company.replace("-", " ").title()
                company_identifier = (job.get("company") or {}).get("identifier", "") or company
                location_data = job.get("location") or {}
                location_text = ""
                if location_data.get("remote"):
                    location_text = "Remote"
                else:
                    parts = [
                        location_data.get("city"),
                        location_data.get("region"),
                        location_data.get("country"),
                    ]
                    location_text = ", ".join([p for p in parts if p])
                posted_date = job.get("releasedDate", "")
                posting_id = job.get("id", "")
                link = ""
                if posting_id:
                    link = f"https://jobs.smartrecruiters.com/{company_identifier}/{posting_id}"
                jobs.append(
                    {
                        "title": title,
                        "company": company_name,
                        "location": location_text,
                        "link": link,
                        "posted_text": "",
                        "posted_date": posted_date,
                    }
                )

            total_found = data.get("totalFound")
            if not isinstance(total_found, int):
                break
            offset += limit
            if offset >= total_found:
                break
            time.sleep(0.2)
    return jobs


def build_email_html(records: List[JobRecord], window_hours: int) -> str:
    header = f"Daily Job Digest · Last {window_hours} hours"
    if not records:
        return (
            "<div style='font-family:Arial, sans-serif; max-width:900px; margin:0 auto;'>"
            f"<h2 style='color:#0B4F8A;'>{header}</h2>"
            f"<p style='color:#333;'>Preferences: {PREFERENCES}</p>"
            f"<p style='color:#333;'>Sources checked: {SOURCES_SUMMARY}</p>"
            "<div style='background:#F7F9FC; padding:16px; border-radius:8px;'>"
            "<p style='margin:0;'>No roles matched in this window. I will keep scanning and send the next update tomorrow.</p>"
            "</div>"
            "</div>"
        )

    top_pick = select_top_pick(records)

    top_pick_section = ""
    if top_pick:
        top_pick_section = (
            "<div style='border:1px solid #F3C969; border-left:6px solid #F5A623; "
            "background:#FFF8E6; padding:12px; border-radius:8px; margin-bottom:14px;'>"
            "<div style='font-weight:bold; color:#8A5A0B; margin-bottom:6px;'>Top Pick</div>"
            f"<div style='font-size:16px; font-weight:bold; color:#0B4F8A;'>"
            f"<a href='{top_pick.link}' style='color:#0B4F8A; text-decoration:none;'>"
            f"{top_pick.role}</a></div>"
            f"<div style='color:#555; margin-top:4px;'>{top_pick.company} · {top_pick.location}</div>"
        f"<div style='margin-top:8px; color:#333;'><strong>Released:</strong> {top_pick.posted} "
        f"· <strong>Source:</strong> {top_pick.source} · <strong>Fit:</strong> {top_pick.fit_score}%</div>"
            f"<div style='margin-top:8px; color:#333;'><strong>Preference match:</strong> "
            f"{top_pick.preference_match}</div>"
            f"<div style='margin-top:8px; color:#333;'><strong>Why you fit:</strong> "
            f"{top_pick.why_fit}</div>"
            f"<div style='margin-top:8px; color:#333;'><strong>Potential gaps:</strong> "
            f"{top_pick.cv_gap}</div>"
            "</div>"
        )

    rows = []
    for idx, rec in enumerate(records):
        if rec.fit_score >= 85:
            fit_color = "#1B7F5D"
        elif rec.fit_score >= 75:
            fit_color = "#2B6CB0"
        else:
            fit_color = "#8A5A0B"

        row_bg = "#FFFFFF" if idx % 2 == 0 else "#F9FBFD"
        if top_pick and rec.link == top_pick.link:
            row_bg = "#FFF3D6"
        badge = ""
        if top_pick and rec.link == top_pick.link:
            badge = (
                "<span style='display:inline-block; margin-left:8px; padding:2px 6px; "
                "border-radius:10px; background:#F5A623; color:#fff; font-size:11px; "
                "font-weight:bold;'>Top Pick</span>"
            )

        rows.append(
            f"<tr style='background:{row_bg};'>"
            f"<td style='padding:10px;'><a href='{rec.link}' style='color:#0B4F8A; text-decoration:none;'><strong>{rec.role}</strong></a>{badge}"
            f"<div style='color:#666; font-size:12px; margin-top:4px;'>{rec.company} · {rec.location}</div></td>"
            f"<td style='padding:10px; white-space:nowrap;'>{rec.posted}</td>"
            f"<td style='padding:10px; color:#333;'>{rec.source}</td>"
            f"<td style='padding:10px;'><span style='display:inline-block; padding:4px 8px; border-radius:12px; "
            f"background:{fit_color}; color:#fff; font-weight:bold;'>{rec.fit_score}%</span></td>"
            f"<td style='padding:10px; color:#333;'>{rec.preference_match}</td>"
            f"<td style='padding:10px; color:#333;'>{rec.why_fit}</td>"
            f"<td style='padding:10px; color:#333;'>{rec.cv_gap}</td>"
            "</tr>"
        )

    table = (
        "<table style='width:100%; border-collapse:collapse; font-family:Arial, sans-serif; "
        "border:1px solid #E5E9F0;'>"
        "<thead style='background:#F0F4F8;'>"
        "<tr>"
        "<th style='text-align:left; padding:10px;'>Role</th>"
            "<th style='text-align:left; padding:10px;'>Released</th>"
            "<th style='text-align:left; padding:10px;'>Source</th>"
            "<th style='text-align:left; padding:10px;'>Fit</th>"
            "<th style='text-align:left; padding:10px;'>Preference Match</th>"
            "<th style='text-align:left; padding:10px;'>Why You Fit</th>"
            "<th style='text-align:left; padding:10px;'>Potential Gaps</th>"
        "</tr>"
        "</thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )

    return (
        "<div style='font-family:Arial, sans-serif; max-width:1000px; margin:0 auto;'>"
        f"<h2 style='color:#0B4F8A; margin-bottom:4px;'>{header}</h2>"
        f"<p style='color:#555; margin-top:0;'>Preferences: {PREFERENCES}</p>"
        f"<p style='color:#555; margin-top:0;'>Sources checked: {SOURCES_SUMMARY}</p>"
        f"<p style='color:#333; font-weight:bold;'>Matches found: {len(records)}</p>"
        + top_pick_section
        + table
        + "</div>"
    )


def send_email(subject: str, html_body: str, text_body: str) -> bool:
    if not EMAIL_ENABLED:
        print("Email disabled: JOB_DIGEST_EMAIL_ENABLED=false")
        return False
    if not all([SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, FROM_EMAIL, TO_EMAIL]):
        print("Email not configured: missing SMTP settings")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = FROM_EMAIL
    msg["To"] = TO_EMAIL

    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    import smtplib

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        print("Email sent successfully")
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"Email send failed: {exc}")
        return False


def main() -> None:
    if not should_run_now():
        print("Skipping run: outside scheduled run window or already sent today.")
        return

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    all_jobs: List[JobRecord] = []

    linkedin_jobs = linkedin_search(session)
    for job in linkedin_jobs:
        title = job.get("title", "")
        company = job.get("company", "")
        location = job.get("location", "")
        if not is_relevant_title(title):
            continue
        if not is_relevant_location(location):
            continue

        desc_text, posted_detail, detail_location = linkedin_job_details(session, job["job_id"])
        if detail_location:
            location = detail_location

        posted_text = posted_detail or job.get("posted_text", "")
        if not parse_posted_within_window(posted_text, job.get("posted_date", ""), WINDOW_HOURS):
            continue

        full_text = f"{title} {desc_text}"
        score, _, _ = score_fit(full_text, company)

        if score < MIN_SCORE:
            continue

        why_fit = build_reasons(full_text)
        cv_gap = build_gaps(full_text)
        preference_match = build_preference_match(full_text, company, location)

        all_jobs.append(
            JobRecord(
                role=title,
                company=company,
                location=location,
                link=job.get("link", ""),
                posted=posted_text,
                source="LinkedIn",
                fit_score=score,
                preference_match=preference_match,
                why_fit=why_fit,
                cv_gap=cv_gap,
                notes="",
            )
        )

    greenhouse_jobs = greenhouse_search(session)
    for job in greenhouse_jobs:
        title = job.get("title", "")
        company = job.get("company", "")
        location = job.get("location", "")
        if not is_relevant_title(title):
            continue
        if not is_relevant_location(location):
            continue

        posted_text = job.get("posted_text", "")
        posted_date = job.get("posted_date", "")
        if not parse_posted_within_window(posted_text, posted_date, WINDOW_HOURS):
            continue

        full_text = f"{title} {company}"
        score, _, _ = score_fit(full_text, company)
        if score < MIN_SCORE:
            continue

        why_fit = build_reasons(full_text)
        cv_gap = build_gaps(full_text)
        preference_match = build_preference_match(full_text, company, location)

        all_jobs.append(
            JobRecord(
                role=title,
                company=company,
                location=location,
                link=job.get("link", ""),
                posted=posted_text or posted_date,
                source="Greenhouse",
                fit_score=score,
                preference_match=preference_match,
                why_fit=why_fit,
                cv_gap=cv_gap,
                notes="",
            )
        )

    lever_jobs = lever_search(session)
    for job in lever_jobs:
        title = job.get("title", "")
        company = job.get("company", "")
        location = job.get("location", "")
        if not is_relevant_title(title):
            continue
        if not is_relevant_location(location):
            continue

        posted_text = job.get("posted_text", "")
        posted_date = job.get("posted_date", "")
        if not parse_posted_within_window(posted_text, posted_date, WINDOW_HOURS):
            continue

        full_text = f"{title} {company}"
        score, _, _ = score_fit(full_text, company)
        if score < MIN_SCORE:
            continue

        why_fit = build_reasons(full_text)
        cv_gap = build_gaps(full_text)
        preference_match = build_preference_match(full_text, company, location)

        all_jobs.append(
            JobRecord(
                role=title,
                company=company,
                location=location,
                link=job.get("link", ""),
                posted=posted_text or posted_date,
                source="Lever",
                fit_score=score,
                preference_match=preference_match,
                why_fit=why_fit,
                cv_gap=cv_gap,
                notes="",
            )
        )

    smart_jobs = smartrecruiters_search(session)
    for job in smart_jobs:
        title = job.get("title", "")
        company = job.get("company", "")
        location = job.get("location", "")
        if not is_relevant_title(title):
            continue
        if not is_relevant_location(location):
            continue

        posted_text = job.get("posted_text", "")
        posted_date = job.get("posted_date", "")
        if not parse_posted_within_window(posted_text, posted_date, WINDOW_HOURS):
            continue

        full_text = f"{title} {company}"
        score, _, _ = score_fit(full_text, company)
        if score < MIN_SCORE:
            continue

        why_fit = build_reasons(full_text)
        cv_gap = build_gaps(full_text)
        preference_match = build_preference_match(full_text, company, location)

        all_jobs.append(
            JobRecord(
                role=title,
                company=company,
                location=location,
                link=job.get("link", ""),
                posted=posted_text or posted_date,
                source="SmartRecruiters",
                fit_score=score,
                preference_match=preference_match,
                why_fit=why_fit,
                cv_gap=cv_gap,
                notes="",
            )
        )

    # Sort and dedupe by link
    unique: Dict[str, JobRecord] = {}
    for job in sorted(all_jobs, key=lambda x: x.fit_score, reverse=True):
        if job.link not in unique:
            unique[job.link] = job

    records = list(unique.values())

    # Keep records ordered by fit score (highest first)
    records = sorted(records, key=lambda record: record.fit_score, reverse=True)

    # Remove roles already sent in previous digests
    seen_cache = prune_seen_cache(load_seen_cache(SEEN_CACHE_PATH), SEEN_CACHE_DAYS)
    records = filter_new_records(records, seen_cache)
    records = sorted(records, key=lambda record: record.fit_score, reverse=True)

    # Write outputs
    today = datetime.now().strftime("%Y-%m-%d")
    out_xlsx = DIGEST_DIR / f"digest_{today}.xlsx"
    out_csv = DIGEST_DIR / f"digest_{today}.csv"

    df = pd.DataFrame([
        {
            "Role": r.role,
            "Company": r.company,
            "Location": r.location,
            "Link": r.link,
            "Posted": r.posted,
            "Source": r.source,
            "Fit_Score_%": r.fit_score,
            "Preference_Match": r.preference_match,
            "Why_Fit": r.why_fit,
            "CV_Gap": r.cv_gap,
            "Notes": r.notes,
        }
        for r in records
    ])

    if not df.empty:
        df.to_excel(out_xlsx, index=False)
        df.to_csv(out_csv, index=False)
    else:
        # still create empty files to track runs
        df.to_excel(out_xlsx, index=False)
        df.to_csv(out_csv, index=False)

    # Build and send email
    top_pick = select_top_pick(records)
    top_records = records[:MAX_EMAIL_ROLES]
    if top_pick and top_pick not in top_records:
        top_records = [top_pick] + top_records
        top_records = top_records[:MAX_EMAIL_ROLES]
    html_body = build_email_html(top_records, WINDOW_HOURS)
    text_lines = [
        f"Daily job digest (last {WINDOW_HOURS} hours).",
        f"Preferences: {PREFERENCES}",
        f"Sources checked: {SOURCES_SUMMARY}",
        f"Roles found: {len(records)}",
        "",
    ]
    if top_pick:
        text_lines.append("Top pick:")
        text_lines.append(
            f"- {top_pick.role} | {top_pick.company} | {top_pick.posted} | "
            f"Source {top_pick.source} | Fit {top_pick.fit_score}%"
        )
        text_lines.append(f"  Preference match: {top_pick.preference_match}")
        text_lines.append(f"  Why fit: {top_pick.why_fit}")
        text_lines.append(f"  Potential gaps: {top_pick.cv_gap}")
        text_lines.append(f"  Link: {top_pick.link}")
        text_lines.append("")
    for rec in top_records:
        text_lines.append(
            f"- {rec.role} | {rec.company} | {rec.posted} | "
            f"Source {rec.source} | Fit {rec.fit_score}%"
        )
        text_lines.append(f"  Preference match: {rec.preference_match}")
        text_lines.append(f"  Why fit: {rec.why_fit}")
        text_lines.append(f"  Potential gaps: {rec.cv_gap}")
        text_lines.append(f"  Link: {rec.link}")
        text_lines.append("")
    text_body = "\n".join(text_lines)

    subject = f"Daily Job Digest - {today}"
    email_sent = send_email(subject, html_body, text_body)

    if email_sent:
        for record in records:
            if record.link:
                seen_cache[record.link] = now_utc().isoformat()
        save_seen_cache(SEEN_CACHE_PATH, seen_cache)
        if RUN_AT:
            state = load_run_state(RUN_STATE_PATH)
            state["last_run_date"] = datetime.now().strftime("%Y-%m-%d")
            save_run_state(RUN_STATE_PATH, state)

    print(f"Digest generated: {out_xlsx}")
    print(f"Roles found: {len(records)}")


if __name__ == "__main__":
    main()
