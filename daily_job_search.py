#!/usr/bin/env python3
"""
Daily job search and email digest for KYC/AML/onboarding product roles.
Sources: LinkedIn guest endpoints, Greenhouse boards (optional).
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote_plus, urljoin

try:
    from zoneinfo import ZoneInfo
except Exception:  # noqa: BLE001
    ZoneInfo = None

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    import feedparser
except Exception:  # noqa: BLE001
    feedparser = None

try:
    import google.generativeai as genai
except Exception:  # noqa: BLE001
    genai = None

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:  # noqa: BLE001
    firebase_admin = None
    credentials = None
    firestore = None


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
    prep_questions: List[str] = field(default_factory=list)
    apply_tips: str = ""


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
SOURCES_SUMMARY_OVERRIDE = os.getenv("JOB_DIGEST_SOURCES", "")
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

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "") or os.getenv("JOB_DIGEST_GEMINI_KEY", "")
GEMINI_MODEL = os.getenv("JOB_DIGEST_GEMINI_MODEL", "gemini-1.5-flash")
GEMINI_MAX_JOBS = int(os.getenv("JOB_DIGEST_GEMINI_MAX_JOBS", "20"))
JOB_DIGEST_PROFILE = os.getenv(
    "JOB_DIGEST_PROFILE",
    "Global product/process owner with KYC, onboarding, screening, financial crime, and"
    " compliance transformation experience across banks and RegTech platforms.",
)

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID", "") or os.getenv("JOB_DIGEST_ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "") or os.getenv("JOB_DIGEST_ADZUNA_APP_KEY", "")
JOOBLE_API_KEY = os.getenv("JOOBLE_API_KEY", "") or os.getenv("JOB_DIGEST_JOOBLE_KEY", "")
REED_API_KEY = os.getenv("REED_API_KEY", "") or os.getenv("JOB_DIGEST_REED_KEY", "")
CV_LIBRARY_API_KEY = os.getenv("CV_LIBRARY_API_KEY", "") or os.getenv("JOB_DIGEST_CVLIB_KEY", "")

FIREBASE_SERVICE_ACCOUNT_JSON = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON", "")
FIREBASE_SERVICE_ACCOUNT_B64 = os.getenv("FIREBASE_SERVICE_ACCOUNT_B64", "")
FIREBASE_COLLECTION = os.getenv("FIREBASE_COLLECTION", "jobs")

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

BOARD_KEYWORDS = [
    "product manager onboarding",
    "product manager kyc",
    "product manager aml",
    "product manager compliance",
    "product manager screening",
    "product owner onboarding",
    "product manager fraud",
    "product manager financial crime",
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
    "wise",
    "revolut",
    "monzo",
    "starlingbank",
    "clearbank",
    "oaknorth",
    "tide",
    "chip",
    "kroo",
    "curve",
    "fundingcircle",
    "lendable",
    "lexisnexis",
    "dowjones",
    "saphyre",
    "alloy",
    "finch",
    "snyk",
    "clearscore",
    "starlingbank",
    "tide",
    "truelayer",
    "mambu",
    "thoughtmachine",
    "rapyd",
    "plaid",
    "marqeta",
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
    "plaid",
    "marqeta",
    "fundingcircle",
    "lendable",
    "smartpension",
    "snyk",
    "checkoutcom",
    "worldremit",
    "azimo",
    "mambu",
    "thoughtmachine",
    "fenergo",
    "quantexa",
    "complyadvantage",
    "ripjar",
    "napier",
    "symphonyai",
    "lexisnexis",
    "actimize",
    "saphyre",
    "encompass",
]

SMARTRECRUITERS_COMPANIES = [
    "Visa",
    "Mastercard",
    "StandardChartered",
    "BNPParibas",
    "Citi",
    "UBS",
    "DeutscheBank",
    "LSEG",
    "SAP",
    "Oracle",
    "FIS",
    "Moody",
    "S&PGlobal",
    "NICE",
    "DowJones",
    "Barclays",
    "HSBC",
    "Lloyds",
    "NatWest",
    "Santander",
]

ASHBY_BOARDS = [
    "ramp",
    "brex",
    "mercury",
    "airwallex",
    "gocardless",
    "stripe",
    "checkout",
    "klarna",
    "wise",
    "revolut",
    "plaid",
    "marqeta",
    "truelayer",
    "mambu",
    "thoughtmachine",
]

EXTRA_GREENHOUSE = [x.strip() for x in os.getenv("JOB_DIGEST_GREENHOUSE_BOARDS", "").split(",") if x.strip()]
EXTRA_LEVER = [x.strip() for x in os.getenv("JOB_DIGEST_LEVER_BOARDS", "").split(",") if x.strip()]
EXTRA_SMARTRECRUITERS = [
    x.strip() for x in os.getenv("JOB_DIGEST_SMARTRECRUITERS", "").split(",") if x.strip()
]
EXTRA_ASHBY = [x.strip() for x in os.getenv("JOB_DIGEST_ASHBY_BOARDS", "").split(",") if x.strip()]

def dedupe_keep_order(items: List[str]) -> List[str]:
    seen: set[str] = set()
    deduped: List[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


for board in EXTRA_GREENHOUSE:
    if board not in GREENHOUSE_BOARDS:
        GREENHOUSE_BOARDS.append(board)
for board in EXTRA_LEVER:
    if board not in LEVER_BOARDS:
        LEVER_BOARDS.append(board)
for company in EXTRA_SMARTRECRUITERS:
    if company not in SMARTRECRUITERS_COMPANIES:
        SMARTRECRUITERS_COMPANIES.append(company)
for board in EXTRA_ASHBY:
    if board not in ASHBY_BOARDS:
        ASHBY_BOARDS.append(board)

GREENHOUSE_BOARDS = dedupe_keep_order(GREENHOUSE_BOARDS)
LEVER_BOARDS = dedupe_keep_order(LEVER_BOARDS)
SMARTRECRUITERS_COMPANIES = dedupe_keep_order(SMARTRECRUITERS_COMPANIES)
ASHBY_BOARDS = dedupe_keep_order(ASHBY_BOARDS)

JOB_BOARD_SOURCES = [
    {"name": "WeWorkRemotely", "type": "rss", "url": "https://weworkremotely.com/categories/remote-product-jobs.rss"},
    {"name": "Remotive", "type": "api", "url": "https://remotive.com/api/remote-jobs"},
    {"name": "RemoteOK", "type": "api", "url": "https://remoteok.com/api"},
    {"name": "WorkAnywhere", "type": "rss", "url": "https://workanywhere.io/jobs.rss"},
    {"name": "RemoteYeah", "type": "rss", "url": "https://remoteyeah.com/jobs.rss"},
    {"name": "Jobicy", "type": "api", "url": "https://jobicy.com/api/v2/remote-jobs"},
    {"name": "MeetFrank", "type": "api", "url": "https://api.meetfrank.com/ai/jobs"},
    {"name": "Empllo", "type": "rss", "url": "https://empllo.com/rss/remote-product-jobs.rss"},
    {"name": "JobsCollider", "type": "rss", "url": "https://jobscollider.com/remote-jobs.rss"},
    {"name": "RealWorkFromAnywhere", "type": "rss", "url": "https://www.realworkfromanywhere.com/rss.xml"},
    {"name": "WorkAnywherePro", "type": "rss", "url": "https://workanywhere.pro/rss.xml"},
    {"name": "Adzuna", "type": "api", "url": "https://api.adzuna.com/v1/api/jobs/gb/search/1"},
    {"name": "Jooble", "type": "api", "url": "https://jooble.org/api"},
    {"name": "Reed", "type": "api", "url": "https://www.reed.co.uk/api/1.0/search"},
    {"name": "CVLibrary", "type": "api", "url": "https://www.cv-library.co.uk/search-jobs-json"},
    {"name": "Totaljobs", "type": "html", "url": "https://www.totaljobs.com"},
    {"name": "CWJobs", "type": "html", "url": "https://www.cwjobs.co.uk"},
    {"name": "Jobsite", "type": "html", "url": "https://www.jobsite.co.uk"},
    {"name": "Technojobs", "type": "html", "url": "https://www.technojobs.co.uk"},
    {"name": "BuiltInLondon", "type": "html", "url": "https://builtinlondon.uk"},
    {"name": "eFinancialCareers", "type": "html", "url": "https://www.efinancialcareers.co.uk"},
    {"name": "IndeedUK", "type": "html", "url": "https://uk.indeed.com"},
    {"name": "JobServe", "type": "html", "url": "https://jobserve.com/gb/en/Job-Search/"},
]

JOB_BOARD_URLS = {source["name"]: source["url"] for source in JOB_BOARD_SOURCES}


def build_sources_summary() -> str:
    if SOURCES_SUMMARY_OVERRIDE:
        return SOURCES_SUMMARY_OVERRIDE

    board_names = [source["name"] for source in JOB_BOARD_SOURCES]
    boards_summary = f"Job boards ({len(board_names)}): " + ", ".join(board_names)

    ats_summary = (
        "ATS boards: "
        f"Greenhouse ({len(GREENHOUSE_BOARDS)}), "
        f"Lever ({len(LEVER_BOARDS)}), "
        f"SmartRecruiters ({len(SMARTRECRUITERS_COMPANIES)}), "
        f"Ashby ({len(ASHBY_BOARDS)})"
    )

    summary = " · ".join(
        [
            "LinkedIn (guest search + company search)",
            boards_summary,
            ats_summary,
        ]
    )

    missing_keys = []
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
        missing_keys.append("Adzuna")
    if not JOOBLE_API_KEY:
        missing_keys.append("Jooble")
    if not REED_API_KEY:
        missing_keys.append("Reed")
    if not CV_LIBRARY_API_KEY:
        missing_keys.append("CVLibrary")
    if missing_keys:
        summary = f"{summary} · APIs pending: {', '.join(missing_keys)}"

    return summary


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


RELATIVE_DATE_REGEX = re.compile(
    r"(reposted\s+\d+\s+days?\s+ago|\d+\s+days?\s+ago|\d+\s+hours?\s+ago|\d+\s+minutes?\s+ago|yesterday|today|new)",
    re.IGNORECASE,
)


def extract_relative_posted_text(text: str) -> str:
    match = RELATIVE_DATE_REGEX.search(text)
    if not match:
        return ""
    value = match.group(1).lower().strip()
    if value == "new":
        return "today"
    return value


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
            cleaned = posted_date.replace("Z", "+00:00")
            dt = datetime.fromisoformat(cleaned)
        except ValueError:
            try:
                dt = parsedate_to_datetime(posted_date)
            except (TypeError, ValueError):
                if posted_date.isdigit():
                    try:
                        ts = int(posted_date)
                        if ts > 10_000_000_000:
                            ts = ts / 1000
                        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    except (ValueError, OSError):
                        return False
                else:
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


def parse_gemini_payload(text: str) -> Optional[Dict[str, object]]:
    if not text:
        return None
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def enhance_records_with_gemini(records: List[JobRecord]) -> List[JobRecord]:
    if not GEMINI_API_KEY or genai is None:
        return records

    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel(GEMINI_MODEL)
    except Exception:
        return records

    limit = min(GEMINI_MAX_JOBS, len(records))
    for record in records[:limit]:
        prompt = (
            "You are a senior UK fintech product recruiter. Given the candidate profile and job summary, "
            "score fit 0-100 and produce concise notes. Return JSON ONLY with keys: "
            "fit_score (int), why_fit (string), cv_gap (string), prep_questions (array of 3-5 strings), "
            "apply_tips (string).\n\n"
            f"Candidate profile: {JOB_DIGEST_PROFILE}\n"
            f"Preferences: {PREFERENCES}\n\n"
            "Job:\n"
            f"Title: {record.role}\n"
            f"Company: {record.company}\n"
            f"Location: {record.location}\n"
            f"Posted: {record.posted}\n"
            f"Notes: {record.notes}\n"
        )
        try:
            response = model.generate_content(prompt)
        except Exception:
            continue

        data = parse_gemini_payload(getattr(response, "text", "") or "")
        if not data:
            continue

        try:
            fit_score = int(data.get("fit_score", record.fit_score))
        except (TypeError, ValueError):
            fit_score = record.fit_score
        record.fit_score = max(0, min(100, fit_score))
        record.why_fit = data.get("why_fit", record.why_fit) or record.why_fit
        record.cv_gap = data.get("cv_gap", record.cv_gap) or record.cv_gap
        prep_questions = data.get("prep_questions", record.prep_questions)
        if isinstance(prep_questions, str):
            prep_questions = [prep_questions]
        if isinstance(prep_questions, list):
            record.prep_questions = [str(q).strip() for q in prep_questions if str(q).strip()]
        record.apply_tips = data.get("apply_tips", record.apply_tips) or record.apply_tips

        time.sleep(0.25)

    return records


def init_firestore_client() -> Optional["firestore.Client"]:
    if firebase_admin is None or credentials is None or firestore is None:
        return None
    if not FIREBASE_SERVICE_ACCOUNT_JSON and not FIREBASE_SERVICE_ACCOUNT_B64:
        return None

    try:
        if FIREBASE_SERVICE_ACCOUNT_JSON:
            service_data = json.loads(FIREBASE_SERVICE_ACCOUNT_JSON)
        else:
            decoded = base64.b64decode(FIREBASE_SERVICE_ACCOUNT_B64).decode("utf-8")
            service_data = json.loads(decoded)
    except (ValueError, json.JSONDecodeError, OSError):
        return None

    try:
        if not firebase_admin._apps:
            firebase_admin.initialize_app(credentials.Certificate(service_data))
        return firestore.client()
    except Exception:
        return None


def record_document_id(record: JobRecord) -> str:
    seed = record.link or f"{record.company}-{record.role}-{record.location}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return digest[:24]


def write_records_to_firestore(records: List[JobRecord]) -> None:
    client = init_firestore_client()
    if client is None:
        return

    for record in records:
        doc_id = record_document_id(record)
        data = {
            "role": record.role,
            "company": record.company,
            "location": record.location,
            "link": record.link,
            "posted": record.posted,
            "source": record.source,
            "fit_score": record.fit_score,
            "preference_match": record.preference_match,
            "why_fit": record.why_fit,
            "cv_gap": record.cv_gap,
            "notes": record.notes,
            "prep_questions": record.prep_questions,
            "apply_tips": record.apply_tips,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            client.collection(FIREBASE_COLLECTION).document(doc_id).set(data, merge=True)
        except Exception:
            continue


def run_smoke_test() -> None:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    results: Dict[str, Dict[str, int]] = {}

    # LinkedIn single-request probe
    linkedin_count = 0
    linkedin_status = 0
    try:
        resp = session.get(
            "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search",
            params={
                "keywords": "product manager onboarding",
                "location": "London, United Kingdom",
                "f_TPR": "r604800",
                "start": 0,
            },
            timeout=20,
        )
        linkedin_status = resp.status_code
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            linkedin_count = len(soup.select("div.base-search-card"))
    except requests.RequestException:
        linkedin_status = 0

    results["LinkedIn"] = {"count": linkedin_count, "status": linkedin_status}

    # Greenhouse boards
    gh_success = 0
    gh_total = len(GREENHOUSE_BOARDS)
    gh_jobs = 0
    for board in GREENHOUSE_BOARDS:
        url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs"
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
        gh_success += 1
        gh_jobs += len(data.get("jobs", []))
    results["Greenhouse"] = {"count": gh_jobs, "status": gh_success}

    # Lever boards
    lever_success = 0
    lever_total = len(LEVER_BOARDS)
    lever_jobs = 0
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
        if isinstance(data, list):
            lever_success += 1
            lever_jobs += len(data)
    results["Lever"] = {"count": lever_jobs, "status": lever_success}

    # SmartRecruiters companies
    sr_success = 0
    sr_total = len(SMARTRECRUITERS_COMPANIES)
    sr_jobs = 0
    for company in SMARTRECRUITERS_COMPANIES:
        url = f"https://api.smartrecruiters.com/v1/companies/{company}/postings"
        try:
            resp = session.get(url, params={"limit": 20, "offset": 0}, timeout=20)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        sr_success += 1
        sr_jobs += len(data.get("content", []))
    results["SmartRecruiters"] = {"count": sr_jobs, "status": sr_success}

    # Ashby boards
    ashby_success = 0
    ashby_total = len(ASHBY_BOARDS)
    ashby_jobs = 0
    for board in ASHBY_BOARDS:
        url = f"https://api.ashbyhq.com/posting-api/job-board/{board}"
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
        postings = data.get("jobs") or data.get("postings") or []
        if isinstance(postings, list):
            ashby_success += 1
            ashby_jobs += len(postings)
    results["Ashby"] = {"count": ashby_jobs, "status": ashby_success}

    # Job board feeds/APIs
    board_results: Dict[str, Dict[str, int]] = {}
    for source in JOB_BOARD_SOURCES:
        count = 0
        status = 1
        if source["type"] == "rss":
            if feedparser is not None:
                feed = feedparser.parse(source["url"])
                count = len(feed.entries)
        elif source["type"] == "api":
            if source["name"] == "Remotive":
                jobs = remotive_search(session)
                count = len(jobs)
            elif source["name"] == "RemoteOK":
                jobs = remoteok_search(session)
                count = len(jobs)
            elif source["name"] == "Jobicy":
                jobs = jobicy_search(session)
                count = len(jobs)
            elif source["name"] == "MeetFrank":
                jobs = meetfrank_search(session)
                count = len(jobs)
            elif source["name"] == "Adzuna":
                if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
                    status = 0
                else:
                    jobs = adzuna_search(session)
                    count = len(jobs)
            elif source["name"] == "Jooble":
                if not JOOBLE_API_KEY:
                    status = 0
                else:
                    jobs = jooble_search(session)
                    count = len(jobs)
            elif source["name"] == "Reed":
                if not REED_API_KEY:
                    status = 0
                else:
                    jobs = reed_search(session)
                    count = len(jobs)
            elif source["name"] == "CVLibrary":
                if not CV_LIBRARY_API_KEY:
                    status = 0
                else:
                    jobs = cvlibrary_search(session)
                    count = len(jobs)
        elif source["type"] == "html":
            if source["name"] == "JobServe":
                jobs = jobserve_search(session)
                count = len(jobs)
            elif source["name"] == "Totaljobs":
                jobs = html_board_search(session, "Totaljobs", source["url"])
                count = len(jobs)
            elif source["name"] == "CWJobs":
                jobs = html_board_search(session, "CWJobs", source["url"])
                count = len(jobs)
            elif source["name"] == "Jobsite":
                jobs = html_board_search(session, "Jobsite", source["url"])
                count = len(jobs)
            elif source["name"] == "Technojobs":
                jobs = technojobs_search(session)
                count = len(jobs)
            elif source["name"] == "BuiltInLondon":
                jobs = builtin_london_search(session)
                count = len(jobs)
            elif source["name"] == "eFinancialCareers":
                jobs = efinancialcareers_search(session)
                count = len(jobs)
            elif source["name"] == "IndeedUK":
                jobs = indeed_search(session)
                count = len(jobs)
        board_results[source["name"]] = {"count": count, "status": status}

    results.update(board_results)

    print("Smoke test summary (counts are raw postings, not filtered):")
    print(json.dumps(results, indent=2))
    print("")
    print(f"Greenhouse boards reachable: {gh_success}/{gh_total}")
    print(f"Lever boards reachable: {lever_success}/{lever_total}")
    print(f"SmartRecruiters companies reachable: {sr_success}/{sr_total}")
    print(f"Ashby boards reachable: {ashby_success}/{ashby_total}")


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


def ashby_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    for board in ASHBY_BOARDS:
        url = f"https://api.ashbyhq.com/posting-api/job-board/{board}"
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
        postings = data.get("jobs") or data.get("postings") or []
        if not isinstance(postings, list):
            continue
        for job in postings:
            title = job.get("title", "")
            if not title:
                continue
            company = job.get("companyName") or board.replace("-", " ").title()
            location = (
                job.get("location")
                or job.get("locationText")
                or job.get("locationName")
                or ""
            )
            link = (
                job.get("jobUrl")
                or job.get("jobPageUrl")
                or job.get("applyUrl")
                or ""
            )
            posted_date = job.get("publishedAt") or job.get("createdAt") or ""
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


def parse_entry_date(entry: Dict[str, str]) -> str:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        return dt.isoformat()
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
        return dt.isoformat()
    return ""


def rss_search(url: str, source_name: str) -> List[Dict[str, str]]:
    if feedparser is None:
        return []
    feed = feedparser.parse(url)
    jobs: List[Dict[str, str]] = []
    for entry in feed.entries:
        title = entry.get("title", "")
        if not title:
            continue
        link = entry.get("link", "")
        summary = normalize_text(entry.get("summary", "")) if entry.get("summary") else ""
        posted_date = parse_entry_date(entry)

        company = entry.get("author", "")
        if " at " in title.lower() and not company:
            parts = title.split(" at ")
            if len(parts) == 2:
                title, company = parts[0].strip(), parts[1].strip()

        jobs.append(
            {
                "title": title,
                "company": company or source_name,
                "location": "Remote",
                "link": clean_link(link),
                "posted_text": "",
                "posted_date": posted_date,
                "summary": summary,
                "source": source_name,
            }
        )
    return jobs


def remotive_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    url = JOB_BOARD_URLS.get("Remotive")
    if not url:
        return jobs
    try:
        resp = session.get(url, timeout=25)
    except requests.RequestException:
        return jobs
    if resp.status_code != 200:
        return jobs
    try:
        data = resp.json()
    except ValueError:
        return jobs
    for job in data.get("jobs", []):
        title = job.get("title", "")
        if not title:
            continue
        jobs.append(
            {
                "title": title,
                "company": job.get("company_name", ""),
                "location": job.get("candidate_required_location", "Remote"),
                "link": job.get("url", ""),
                "posted_text": "",
                "posted_date": job.get("publication_date", ""),
                "summary": normalize_text(job.get("description", "")[:500]),
                "source": "Remotive",
            }
        )
    return jobs


def remoteok_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    url = JOB_BOARD_URLS.get("RemoteOK")
    if not url:
        return jobs
    try:
        resp = session.get(url, timeout=25)
    except requests.RequestException:
        return jobs
    if resp.status_code != 200:
        return jobs
    try:
        data = resp.json()
    except ValueError:
        return jobs
    if not isinstance(data, list):
        return jobs
    for job in data:
        title = job.get("position", "")
        if not title:
            continue
        jobs.append(
            {
                "title": title,
                "company": job.get("company", ""),
                "location": job.get("location", "Remote"),
                "link": job.get("url", ""),
                "posted_text": "",
                "posted_date": job.get("date", ""),
                "summary": normalize_text(job.get("description", "")[:500]),
                "source": "RemoteOK",
            }
        )
    return jobs


def jobicy_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    url = JOB_BOARD_URLS.get("Jobicy")
    if not url:
        return jobs
    params = {"tag": "product", "geo": "uk"}
    try:
        resp = session.get(url, params=params, timeout=25)
    except requests.RequestException:
        return jobs
    if resp.status_code != 200:
        return jobs
    try:
        data = resp.json()
    except ValueError:
        return jobs
    job_list = data.get("jobs") or data.get("data") or []
    for job in job_list:
        title = job.get("jobTitle") or job.get("title") or ""
        if not title:
            continue
        jobs.append(
            {
                "title": title,
                "company": job.get("companyName", "") or job.get("company", ""),
                "location": job.get("jobGeo", "") or job.get("location", "Remote"),
                "link": job.get("url", "") or job.get("jobUrl", ""),
                "posted_text": "",
                "posted_date": job.get("pubDate", "") or job.get("postedDate", ""),
                "summary": normalize_text(job.get("description", "")[:500]),
                "source": "Jobicy",
            }
        )
    return jobs


def meetfrank_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    url = JOB_BOARD_URLS.get("MeetFrank")
    if not url:
        return jobs

    for keyword in BOARD_KEYWORDS[:3]:
        params = {
            "q": keyword,
            "country": "United Kingdom",
            "location": "London",
            "pageSize": 100,
            "language": "en",
        }
        try:
            resp = session.get(url, params=params, timeout=25)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        for job in data.get("jobs", []):
            title = job.get("title", "")
            if not title:
                continue
            jobs.append(
                {
                    "title": title,
                    "company": job.get("company", ""),
                    "location": job.get("location", ""),
                    "link": job.get("applyUrl", "") or job.get("url", ""),
                    "posted_text": "",
                    "posted_date": job.get("publishedAt", ""),
                    "summary": normalize_text((job.get("description") or "")[:500]),
                    "source": "MeetFrank",
                }
            )
        time.sleep(0.2)
    return jobs


def adzuna_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
        return jobs
    url = JOB_BOARD_URLS.get("Adzuna")
    if not url:
        return jobs

    for keyword in BOARD_KEYWORDS[:3]:
        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "what": keyword,
            "where": "London",
            "results_per_page": 50,
            "sort_by": "date",
            "content-type": "application/json",
        }
        try:
            resp = session.get(url, params=params, timeout=25)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        for job in data.get("results", []):
            title = job.get("title", "")
            if not title:
                continue
            company = (job.get("company") or {}).get("display_name", "")
            location = (job.get("location") or {}).get("display_name", "")
            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "link": job.get("redirect_url", ""),
                    "posted_text": "",
                    "posted_date": job.get("created", ""),
                    "summary": normalize_text((job.get("description") or "")[:500]),
                    "source": "Adzuna",
                }
            )
        time.sleep(0.2)
    return jobs


def jooble_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    if not JOOBLE_API_KEY:
        return jobs
    base_url = JOB_BOARD_URLS.get("Jooble")
    if not base_url:
        return jobs
    url = f"{base_url.rstrip('/')}/{JOOBLE_API_KEY}"

    for keyword in BOARD_KEYWORDS[:3]:
        payload = {
            "keywords": keyword,
            "location": "London",
            "page": 1,
            "radius": 20,
        }
        try:
            resp = session.post(url, json=payload, timeout=25)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        for job in data.get("jobs", []) or []:
            title = job.get("title", "")
            if not title:
                continue
            jobs.append(
                {
                    "title": title,
                    "company": job.get("company", ""),
                    "location": job.get("location", ""),
                    "link": job.get("link", "") or job.get("url", ""),
                    "posted_text": "",
                    "posted_date": job.get("updated", "") or job.get("date", ""),
                    "summary": normalize_text((job.get("snippet") or job.get("description") or "")[:500]),
                    "source": "Jooble",
                }
            )
        time.sleep(0.2)
    return jobs


def reed_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    if not REED_API_KEY:
        return jobs
    url = JOB_BOARD_URLS.get("Reed")
    if not url:
        return jobs

    for keyword in BOARD_KEYWORDS[:3]:
        params = {
            "keywords": keyword,
            "locationName": "London",
            "distanceFromLocation": 25,
            "resultsToTake": 50,
            "resultsToSkip": 0,
        }
        try:
            resp = session.get(url, params=params, auth=(REED_API_KEY, ""), timeout=25)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        for job in data.get("results", []) or []:
            title = job.get("jobTitle") or job.get("job_title") or job.get("title") or ""
            if not title:
                continue
            jobs.append(
                {
                    "title": title,
                    "company": job.get("employerName", ""),
                    "location": job.get("locationName", ""),
                    "link": job.get("jobUrl", ""),
                    "posted_text": "",
                    "posted_date": job.get("date", ""),
                    "summary": normalize_text((job.get("jobDescription") or "")[:500]),
                    "source": "Reed",
                }
            )
        time.sleep(0.2)
    return jobs


def cvlibrary_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    if not CV_LIBRARY_API_KEY:
        return jobs
    url = JOB_BOARD_URLS.get("CVLibrary")
    if not url:
        return jobs

    for keyword in BOARD_KEYWORDS[:3]:
        params = {
            "key": CV_LIBRARY_API_KEY,
            "q": keyword,
            "geo": "London",
            "distance": 20,
            "tempperm": "Permanent",
            "perpage": 50,
            "orderby": "date",
        }
        try:
            resp = session.get(url, params=params, timeout=25)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        for job in data.get("jobs", []) or data.get("results", []) or []:
            title = job.get("title") or job.get("job_title") or ""
            if not title:
                continue
            jobs.append(
                {
                    "title": title,
                    "company": job.get("company") or job.get("company_name") or "",
                    "location": job.get("location") or job.get("geo") or "",
                    "link": job.get("job_url") or job.get("joburl") or job.get("url") or "",
                    "posted_text": "",
                    "posted_date": job.get("date") or job.get("posted") or job.get("date_posted") or "",
                    "summary": normalize_text((job.get("description") or job.get("short_description") or "")[:500]),
                    "source": "CVLibrary",
                }
            )
        time.sleep(0.2)
    return jobs


def slugify(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", text.lower())
    return cleaned.strip("-")


def extract_job_links(html: str, base_url: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[Tuple[str, str]] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "")
        if not re.search(r"/job/|/jobs/|jobid=", href):
            continue
        if href.startswith("/"):
            href = urljoin(base_url, href)
        href = clean_link(href)
        if not href or href in seen:
            continue
        title = normalize_text(anchor.get_text(" "))
        if len(title) < 4:
            continue
        seen.add(href)
        links.append((href, title))
    return links


def iter_jobposting_nodes(data: object) -> Iterable[Dict[str, object]]:
    if isinstance(data, dict):
        types = data.get("@type")
        if types:
            if isinstance(types, list) and "JobPosting" in types:
                yield data
            if isinstance(types, str) and types == "JobPosting":
                yield data
        if "@graph" in data:
            yield from iter_jobposting_nodes(data.get("@graph"))
        for value in data.values():
            yield from iter_jobposting_nodes(value)
    elif isinstance(data, list):
        for item in data:
            yield from iter_jobposting_nodes(item)


def parse_job_detail_jsonld(html: str, fallback_title: str = "") -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        if not script.string:
            continue
        try:
            payload = json.loads(script.string.strip())
        except ValueError:
            continue
        for node in iter_jobposting_nodes(payload):
            title = node.get("title") if isinstance(node.get("title"), str) else ""
            company = ""
            hiring_org = node.get("hiringOrganization")
            if isinstance(hiring_org, dict):
                company = hiring_org.get("name") or ""
            location = ""
            job_location = node.get("jobLocation")
            if isinstance(job_location, list) and job_location:
                job_location = job_location[0]
            if isinstance(job_location, dict):
                address = job_location.get("address")
                if isinstance(address, dict):
                    location = ", ".join(
                        part
                        for part in [
                            address.get("addressLocality"),
                            address.get("addressRegion"),
                            address.get("addressCountry"),
                        ]
                        if part
                    )
            posted_date = node.get("datePosted") if isinstance(node.get("datePosted"), str) else ""
            description = node.get("description") if isinstance(node.get("description"), str) else ""
            return {
                "title": title or fallback_title,
                "company": company,
                "location": location,
                "posted_date": posted_date,
                "summary": normalize_text(description)[:800],
            }
    return {}


def html_board_search(
    session: requests.Session,
    source_name: str,
    base_url: str,
    keyword_limit: int = 3,
    max_details: int = 12,
) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    job_map: Dict[str, Dict[str, str]] = {}

    for keyword in BOARD_KEYWORDS[:keyword_limit]:
        slug = slugify(keyword)
        search_url = f"{base_url}/jobs/{slug}/in-london"
        try:
            resp = session.get(search_url, timeout=30)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue

        links = extract_job_links(resp.text, base_url)
        for link, title in links:
            if link in job_map:
                continue
            job_map[link] = {
                "title": title,
                "company": source_name,
                "location": "London",
                "link": link,
                "posted_text": "",
                "posted_date": "",
                "summary": "",
                "source": source_name,
            }

    detail_links = list(job_map.keys())[:max_details]
    for link in detail_links:
        try:
            resp = session.get(link, timeout=30)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        details = parse_job_detail_jsonld(resp.text, job_map[link]["title"])
        if details:
            job_map[link]["title"] = details.get("title") or job_map[link]["title"]
            job_map[link]["company"] = details.get("company") or job_map[link]["company"]
            job_map[link]["location"] = details.get("location") or job_map[link]["location"]
            job_map[link]["posted_date"] = details.get("posted_date") or job_map[link]["posted_date"]
            if details.get("summary"):
                job_map[link]["summary"] = details["summary"]
        time.sleep(0.2)

    jobs.extend(job_map.values())
    return jobs


def iter_job_like_nodes(data: object) -> Iterable[Dict[str, object]]:
    if isinstance(data, dict):
        keys = data.keys()
        if "title" in keys and ("company" in keys or "companyName" in keys or "company_name" in keys):
            yield data
        for value in data.values():
            yield from iter_job_like_nodes(value)
    elif isinstance(data, list):
        for item in data:
            yield from iter_job_like_nodes(item)


def efinancialcareers_api_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    api_url = "https://job-search-ui.efinancialcareers.com/v1/efc/jobs/search"
    for keyword in BOARD_KEYWORDS[:3]:
        payload = {
            "keyword": keyword,
            "location": "London",
            "results_wanted": 50,
            "sort": "date",
            "offset": 0,
        }
        try:
            resp = session.post(api_url, json=payload, timeout=30)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        for node in iter_job_like_nodes(data):
            title = node.get("title") if isinstance(node.get("title"), str) else ""
            if not title:
                continue
            company = ""
            for key in ("companyName", "company_name", "company"):
                if isinstance(node.get(key), str):
                    company = node.get(key) or company
            link = ""
            for key in ("jobUrl", "url", "applyUrl", "job_url"):
                if isinstance(node.get(key), str):
                    link = node.get(key) or link
            link = clean_link(link)
            posted_date = ""
            for key in ("datePosted", "date_posted", "created", "postedDate"):
                if isinstance(node.get(key), str):
                    posted_date = node.get(key) or posted_date
            summary = ""
            for key in ("description", "jobDescription", "summary"):
                if isinstance(node.get(key), str):
                    summary = normalize_text(node.get(key))[:600]
            location = ""
            for key in ("location", "jobLocation", "city"):
                if isinstance(node.get(key), str):
                    location = node.get(key) or location

            jobs.append(
                {
                    "title": title,
                    "company": company or "eFinancialCareers",
                    "location": location or "United Kingdom",
                    "link": link,
                    "posted_text": "",
                    "posted_date": posted_date,
                    "summary": summary,
                    "source": "eFinancialCareers",
                }
            )
        time.sleep(0.2)
    return jobs


def efinancialcareers_html_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    base_url = JOB_BOARD_URLS.get("eFinancialCareers")
    if not base_url:
        return jobs

    job_map: Dict[str, Dict[str, str]] = {}
    for keyword in BOARD_KEYWORDS[:3]:
        slug = slugify(keyword)
        search_url = f"{base_url}/jobs/{slug}"
        try:
            resp = session.get(search_url, timeout=30)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            if "jobs-" not in href or ".id" not in href:
                continue
            if href.startswith("/"):
                href = urljoin(base_url, href)
            href = clean_link(href)
            if not href or href in job_map:
                continue
            title = normalize_text(anchor.get_text(" "))
            if len(title) < 4:
                continue
            job_map[href] = {
                "title": title,
                "company": "eFinancialCareers",
                "location": "United Kingdom",
                "link": href,
                "posted_text": "",
                "posted_date": "",
                "summary": "",
                "source": "eFinancialCareers",
            }
        time.sleep(0.2)

    detail_links = list(job_map.keys())[:10]
    for link in detail_links:
        try:
            resp = session.get(link, timeout=30)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        details = parse_job_detail_jsonld(resp.text, job_map[link]["title"])
        if details:
            job_map[link]["title"] = details.get("title") or job_map[link]["title"]
            job_map[link]["company"] = details.get("company") or job_map[link]["company"]
            job_map[link]["location"] = details.get("location") or job_map[link]["location"]
            job_map[link]["posted_date"] = details.get("posted_date") or job_map[link]["posted_date"]
            if details.get("summary"):
                job_map[link]["summary"] = details["summary"]
        time.sleep(0.2)

    jobs.extend(job_map.values())
    return jobs


def efinancialcareers_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs = efinancialcareers_api_search(session)
    if jobs:
        return jobs
    return efinancialcareers_html_search(session)


def technojobs_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    base_url = JOB_BOARD_URLS.get("Technojobs")
    if not base_url:
        return jobs

    job_map: Dict[str, Dict[str, str]] = {}
    for keyword in BOARD_KEYWORDS[:2]:
        slug = slugify(keyword)
        search_url = f"{base_url}/{slug}-jobs/london"
        try:
            resp = session.get(search_url, timeout=30)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            if "jobid=" not in href and "/job/" not in href:
                continue
            link = urljoin(base_url, href) if href.startswith("/") else href
            link = clean_link(link)
            if not link or link in job_map:
                continue
            title = normalize_text(anchor.get_text(" "))
            if len(title) < 4:
                continue

            posted_text = ""
            container = anchor
            for _ in range(4):
                if not container:
                    break
                text_blob = normalize_text(container.get_text(" "))
                posted_text = extract_relative_posted_text(text_blob)
                if posted_text:
                    break
                container = container.parent

            job_map[link] = {
                "title": title,
                "company": "Technojobs",
                "location": "London",
                "link": link,
                "posted_text": posted_text,
                "posted_date": "",
                "summary": "",
                "source": "Technojobs",
            }
        time.sleep(0.2)

    jobs.extend(job_map.values())
    return jobs


def indeed_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    base_url = JOB_BOARD_URLS.get("IndeedUK")
    if not base_url:
        return jobs

    if feedparser is not None:
        rss_jobs: List[Dict[str, str]] = []
        for keyword in BOARD_KEYWORDS[:2]:
            rss_url = f"{base_url}/rss?q={quote_plus(keyword)}&l=London&sort=date"
            feed = feedparser.parse(rss_url)
            for entry in feed.entries:
                title = entry.get("title", "")
                if not title:
                    continue
                link = entry.get("link", "")
                summary = normalize_text(entry.get("summary", "")) if entry.get("summary") else ""
                posted_date = parse_entry_date(entry)
                rss_jobs.append(
                    {
                        "title": title,
                        "company": entry.get("author", "") or "Indeed",
                        "location": "London",
                        "link": clean_link(link),
                        "posted_text": "",
                        "posted_date": posted_date,
                        "summary": summary,
                        "source": "IndeedUK",
                    }
                )
            time.sleep(0.2)
        if rss_jobs:
            return rss_jobs

    job_map: Dict[str, Dict[str, str]] = {}
    for keyword in BOARD_KEYWORDS[:3]:
        params = {"q": keyword, "l": "London"}
        try:
            resp = session.get(f"{base_url}/jobs", params=params, timeout=30)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("div.job_seen_beacon") or soup.select("a.tapItem")
        if cards:
            for card in cards:
                jk = card.get("data-jk")
                link = ""
                if jk:
                    link = f"{base_url}/viewjob?jk={jk}"
                else:
                    anchor = card.find("a", href=True)
                    if not anchor:
                        continue
                    href = anchor.get("href", "")
                    if "jk=" not in href and "/rc/clk" not in href and "/viewjob" not in href:
                        continue
                    link = urljoin(base_url, href)
                link = clean_link(link)
                if not link or link in job_map:
                    continue
                title_el = card.select_one("h2.jobTitle span") or card.select_one("a[data-jk]")
                title = normalize_text(title_el.get_text(" ")) if title_el else ""
                if len(title) < 4:
                    continue
                company_el = card.select_one("span.companyName")
                location_el = card.select_one("div.companyLocation")
                snippet_el = card.select_one("div.job-snippet")
                posted_el = card.select_one("span.date") or card.select_one("span[aria-label]")

                posted_text = ""
                if posted_el:
                    posted_text = extract_relative_posted_text(posted_el.get_text(" "))
                if not posted_text:
                    posted_text = extract_relative_posted_text(normalize_text(card.get_text(" ")))

                job_map[link] = {
                    "title": title,
                    "company": normalize_text(company_el.get_text(" ")) if company_el else "Indeed",
                    "location": normalize_text(location_el.get_text(" ")) if location_el else "London",
                    "link": link,
                    "posted_text": posted_text,
                    "posted_date": "",
                    "summary": normalize_text(snippet_el.get_text(" ")) if snippet_el else "",
                    "source": "IndeedUK",
                }
        else:
            for anchor in soup.find_all("a", href=True):
                href = anchor.get("href", "")
                if "jk=" not in href:
                    continue
                if "/rc/clk" not in href and "/viewjob" not in href:
                    continue
                link = urljoin(base_url, href)
                link = clean_link(link)
                if not link or link in job_map:
                    continue
                title = normalize_text(anchor.get_text(" "))
                if len(title) < 4:
                    continue
                job_map[link] = {
                    "title": title,
                    "company": "Indeed",
                    "location": "London",
                    "link": link,
                    "posted_text": "",
                    "posted_date": "",
                    "summary": "",
                    "source": "IndeedUK",
                }

        time.sleep(0.2)

    jobs.extend(job_map.values())
    return jobs


def builtin_london_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    base_url = JOB_BOARD_URLS.get("BuiltInLondon")
    if not base_url:
        return jobs

    job_map: Dict[str, Dict[str, str]] = {}
    for keyword in BOARD_KEYWORDS[:3]:
        slug = slugify(keyword)
        search_url = f"{base_url}/jobs/product/search/{slug}"
        try:
            resp = session.get(search_url, timeout=30)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        found_any = False
        script = soup.find("script", id="__NEXT_DATA__")
        if script and script.string:
            try:
                data = json.loads(script.string)
            except ValueError:
                data = {}

            def iter_nodes(obj: object) -> Iterable[Dict[str, object]]:
                if isinstance(obj, dict):
                    if {"title", "companyName", "jobUrl"}.issubset(obj.keys()):
                        yield obj
                    if {"title", "company", "url"}.issubset(obj.keys()):
                        yield obj
                    for val in obj.values():
                        yield from iter_nodes(val)
                elif isinstance(obj, list):
                    for item in obj:
                        yield from iter_nodes(item)

            for node in iter_nodes(data):
                title = node.get("title") if isinstance(node.get("title"), str) else ""
                if not title:
                    continue
                company = ""
                if isinstance(node.get("companyName"), str):
                    company = node.get("companyName")
                elif isinstance(node.get("company"), str):
                    company = node.get("company")
                link = ""
                if isinstance(node.get("jobUrl"), str):
                    link = node.get("jobUrl")
                elif isinstance(node.get("url"), str):
                    link = node.get("url")
                if link and link.startswith("/"):
                    link = urljoin(base_url, link)
                link = clean_link(link)
                if not link or link in job_map:
                    continue
                location = ""
                if isinstance(node.get("location"), str):
                    location = node.get("location")
                elif isinstance(node.get("jobLocation"), str):
                    location = node.get("jobLocation")
                posted_date = ""
                if isinstance(node.get("postedDate"), str):
                    posted_date = node.get("postedDate")
                summary = ""
                if isinstance(node.get("description"), str):
                    summary = normalize_text(node.get("description"))[:500]
                job_map[link] = {
                    "title": title,
                    "company": company or "BuiltIn",
                    "location": location or "London",
                    "link": link,
                    "posted_text": "",
                    "posted_date": posted_date,
                    "summary": summary,
                    "source": "BuiltInLondon",
                }
                found_any = True

        if found_any:
            time.sleep(0.2)
            continue

        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            if "/job/" not in href:
                continue
            link = urljoin(base_url, href) if href.startswith("/") else href
            link = clean_link(link)
            if not link or link in job_map:
                continue
            title = normalize_text(anchor.get_text(" "))
            if len(title) < 4:
                continue

            posted_text = ""
            container = anchor
            for _ in range(4):
                if not container:
                    break
                text_blob = normalize_text(container.get_text(" "))
                posted_text = extract_relative_posted_text(text_blob)
                if posted_text:
                    break
                container = container.parent

            job_map[link] = {
                "title": title,
                "company": "BuiltIn",
                "location": "London",
                "link": link,
                "posted_text": posted_text,
                "posted_date": "",
                "summary": "",
                "source": "BuiltInLondon",
            }
        time.sleep(0.2)

    jobs.extend(job_map.values())
    return jobs


def jobserve_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    url = JOB_BOARD_URLS.get("JobServe")
    if not url:
        return jobs
    try:
        resp = session.get(url, timeout=25)
    except requests.RequestException:
        return jobs
    if resp.status_code != 200:
        return jobs

    soup = BeautifulSoup(resp.text, "html.parser")
    form = soup.select_one("form")
    if not form:
        return jobs

    base_payload = {}
    for inp in soup.select("form input"):
        name = inp.get("name")
        if not name:
            continue
        base_payload[name] = inp.get("value", "")

    action = form.get("action", "")
    post_url = urljoin(url, action)

    job_map: Dict[str, Dict[str, str]] = {}
    for keyword in BOARD_KEYWORDS[:3]:
        payload = dict(base_payload)
        payload["ctl00$main$srch$ctl_qs$txtKey"] = keyword
        payload["ctl00$main$srch$ctl_qs$txtTitle"] = ""
        payload["ctl00$main$srch$ctl_qs$txtLoc"] = "London"

        try:
            resp2 = session.post(post_url, data=payload, timeout=30)
        except requests.RequestException:
            continue
        if resp2.status_code != 200:
            continue

        soup2 = BeautifulSoup(resp2.text, "html.parser")
        shid_el = soup2.select_one("#shid")
        job_ids_el = soup2.select_one("#jobIDs")
        if not shid_el or not job_ids_el:
            continue
        shid = shid_el.get("value", "")
        job_ids_str = job_ids_el.get("value", "")
        if not shid or not job_ids_str:
            continue
        first_segment = job_ids_str.split("%")[0]
        if not first_segment:
            continue

        api_url = f"https://jobserve.com/WebServices/JobSearch.asmx/RetrieveJobs?shid={shid}"
        try:
            resp3 = session.post(api_url, json={"jobIDsStr": first_segment, "pageNum": "1"}, timeout=30)
        except requests.RequestException:
            continue
        if resp3.status_code != 200:
            continue
        try:
            data = resp3.json()
        except ValueError:
            continue
        html = data.get("d", "")
        if not html:
            continue

        soup3 = BeautifulSoup(html, "html.parser")
        for item in soup3.select("div.jobItem"):
            job_id = (item.get("id") or "").strip()
            if not job_id:
                continue
            title_el = item.select_one("h3.jobResultsTitle")
            title = normalize_text(title_el.get_text(" ")) if title_el else ""
            if not title:
                continue
            location_el = item.select_one("p.jobResultsLoc")
            location = normalize_text(location_el.get_text(" ")) if location_el else ""
            posted_el = item.select_one("p.when")
            posted_text = normalize_text(posted_el.get_text(" ")) if posted_el else ""
            job_type_el = item.select_one("p.jobResultsType")
            job_type = normalize_text(job_type_el.get_text(" ")) if job_type_el else ""
            salary_el = item.select_one("p.jobResultsSalary")
            salary = normalize_text(salary_el.get_text(" ")) if salary_el else ""

            summary_parts = [part for part in [job_type, salary] if part]
            summary = " · ".join(summary_parts)

            job_map[job_id] = {
                "title": title,
                "company": "JobServe",
                "location": location or "United Kingdom",
                "link": f"https://jobserve.com/gb/en/JobSearch.aspx?jobid={job_id}",
                "posted_text": posted_text,
                "posted_date": "",
                "summary": summary,
                "source": "JobServe",
            }

        time.sleep(0.2)

    # Enrich a few jobs with detail text
    detail_ids = list(job_map.keys())[:6]
    for job_id in detail_ids:
        api_url = "https://jobserve.com/WebServices/JobSearch.asmx/RetrieveSingleJobDetail"
        try:
            resp = session.post(api_url, json={"id": job_id}, timeout=20)
        except requests.RequestException:
            continue
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except ValueError:
            continue
        detail_html = (data.get("d") or {}).get("JobDetailHtml", "")
        if not detail_html:
            continue
        detail_text = normalize_text(BeautifulSoup(detail_html, "html.parser").get_text(" "))
        if detail_text:
            job_map[job_id]["summary"] = detail_text[:800]
            if "Posted by:" in detail_text:
                try:
                    company = detail_text.split("Posted by:")[1].split("Posted:", 1)[0].strip()
                    if company:
                        job_map[job_id]["company"] = company
                except Exception:
                    pass

        time.sleep(0.2)

    jobs.extend(job_map.values())
    return jobs


def job_board_search(session: requests.Session) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    for source in JOB_BOARD_SOURCES:
        if source["type"] == "rss":
            jobs.extend(rss_search(source["url"], source["name"]))
        elif source["type"] == "api":
            if source["name"] == "Remotive":
                jobs.extend(remotive_search(session))
            elif source["name"] == "RemoteOK":
                jobs.extend(remoteok_search(session))
            elif source["name"] == "Jobicy":
                jobs.extend(jobicy_search(session))
            elif source["name"] == "MeetFrank":
                jobs.extend(meetfrank_search(session))
            elif source["name"] == "Adzuna":
                jobs.extend(adzuna_search(session))
            elif source["name"] == "Jooble":
                jobs.extend(jooble_search(session))
            elif source["name"] == "Reed":
                jobs.extend(reed_search(session))
            elif source["name"] == "CVLibrary":
                jobs.extend(cvlibrary_search(session))
        elif source["type"] == "html":
            if source["name"] == "JobServe":
                jobs.extend(jobserve_search(session))
            elif source["name"] == "Totaljobs":
                jobs.extend(html_board_search(session, "Totaljobs", source["url"]))
            elif source["name"] == "CWJobs":
                jobs.extend(html_board_search(session, "CWJobs", source["url"]))
            elif source["name"] == "Jobsite":
                jobs.extend(html_board_search(session, "Jobsite", source["url"]))
            elif source["name"] == "Technojobs":
                jobs.extend(technojobs_search(session))
            elif source["name"] == "BuiltInLondon":
                jobs.extend(builtin_london_search(session))
            elif source["name"] == "eFinancialCareers":
                jobs.extend(efinancialcareers_search(session))
            elif source["name"] == "IndeedUK":
                jobs.extend(indeed_search(session))
    return jobs


def build_email_html(records: List[JobRecord], window_hours: int) -> str:
    header = f"Daily Job Digest · Last {window_hours} hours"
    if not records:
        return (
            "<div style='font-family:Arial, sans-serif; max-width:900px; margin:0 auto;'>"
            f"<h2 style='color:#0B4F8A;'>{header}</h2>"
            f"<p style='color:#333;'>Preferences: {PREFERENCES}</p>"
            f"<p style='color:#333;'>Sources checked: {build_sources_summary()}</p>"
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
        f"<p style='color:#555; margin-top:0;'>Sources checked: {build_sources_summary()}</p>"
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

        summary = desc_text[:500]
        full_text = f"{title} {company} {summary}"
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
                notes=summary,
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

        summary = job.get("summary", "")
        full_text = f"{title} {company} {summary}"
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
                notes=summary,
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

        summary = job.get("summary", "")
        full_text = f"{title} {company} {summary}"
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
                notes=summary,
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

        summary = job.get("summary", "")
        full_text = f"{title} {company} {summary}"
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
                notes=summary,
            )
        )

    ashby_jobs = ashby_search(session)
    for job in ashby_jobs:
        title = job.get("title", "")
        company = job.get("company", "")
        location = job.get("location", "") or "Remote"
        if not is_relevant_title(title):
            continue
        if not is_relevant_location(location):
            continue

        posted_text = job.get("posted_text", "")
        posted_date = job.get("posted_date", "")
        if not parse_posted_within_window(posted_text, posted_date, WINDOW_HOURS):
            continue

        summary = job.get("summary", "")
        full_text = f"{title} {company} {summary}"
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
                source="Ashby",
                fit_score=score,
                preference_match=preference_match,
                why_fit=why_fit,
                cv_gap=cv_gap,
                notes=summary,
            )
        )

    board_jobs = job_board_search(session)
    for job in board_jobs:
        title = job.get("title", "")
        company = job.get("company", "")
        location = job.get("location", "") or "Remote"
        if not is_relevant_title(title):
            continue
        if not is_relevant_location(location):
            continue

        posted_text = job.get("posted_text", "")
        posted_date = job.get("posted_date", "")
        if not parse_posted_within_window(posted_text, posted_date, WINDOW_HOURS):
            continue

        summary = job.get("summary", "")
        full_text = f"{title} {company} {summary}"
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
                source=job.get("source", "Job board"),
                fit_score=score,
                preference_match=preference_match,
                why_fit=why_fit,
                cv_gap=cv_gap,
                notes=summary,
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

    # Optional Gemini enhancement (fit %, gaps, prep)
    records = enhance_records_with_gemini(records)
    records = sorted(records, key=lambda record: record.fit_score, reverse=True)

    # Optional Firebase persistence
    write_records_to_firestore(records)

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
            "Prep_Questions": " | ".join(r.prep_questions),
            "Apply_Tips": r.apply_tips,
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
        f"Sources checked: {build_sources_summary()}",
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
    parser = argparse.ArgumentParser(description="Daily job digest runner")
    parser.add_argument("--smoke-test", action="store_true", help="Run a source connectivity smoke test")
    args = parser.parse_args()

    if args.smoke_test:
        run_smoke_test()
    else:
        main()
