# Daily Job Digest (Cloud Runner)

This repo runs a daily job search and emails a digest at **08:40 Europe/London**.

## What it does

- Searches for product roles in KYC/AML/onboarding domains.
- Includes roles posted in the last **24 hours** by default (configurable).
- Suppresses repeats using a cache (`sent_links.json`).
- Sends one email per day at 08:40 (daily schedule).
- Includes direct UK boards (Totaljobs, CWJobs, Jobsite, eFinancialCareers, IndeedUK) plus RSS sources.

## Setup (GitHub Actions)

1. Create a new GitHub repo and push this folder.
2. Go to **Settings → Secrets and variables → Actions → New repository secret** and add:
   - `SMTP_HOST` (e.g., `smtp.gmail.com`)
   - `SMTP_PORT` (e.g., `587`)
   - `SMTP_USER` (your Gmail)
   - `SMTP_PASS` (Gmail App Password)
   - `FROM_EMAIL` (your Gmail)
   - `TO_EMAIL` (destination email)
   - Optional job boards:
     - `ADZUNA_APP_ID`
     - `ADZUNA_APP_KEY`
     - `JOOBLE_API_KEY`
     - `REED_API_KEY`
     - `CV_LIBRARY_API_KEY`
   - Optional ATS expansion:
     - `JOB_DIGEST_GREENHOUSE_BOARDS` (comma-separated)
     - `JOB_DIGEST_LEVER_BOARDS` (comma-separated)
     - `JOB_DIGEST_SMARTRECRUITERS` (comma-separated)
     - `JOB_DIGEST_ASHBY_BOARDS` (comma-separated)
   - Optional Workday feeds:
     - `JOB_DIGEST_WORKDAY_SITES` (comma-separated Workday job site URLs; format: `Company Name|https://company.wd3.myworkdayjobs.com/Company_Careers`)
   - Optional enrichment/portal:
     - `GEMINI_API_KEY`
     - `JOB_DIGEST_PROFILE`
     - `FIREBASE_SERVICE_ACCOUNT_JSON`
     - `FIREBASE_COLLECTION`
3. Go to **Actions** and enable workflows.

## Schedule

- Workflow runs every 15 minutes, but only sends at **08:40 Europe/London**.
- The cache persists `sent_links.json` so repeats are not emailed.

## Files

- `daily_job_search.py`
- `requirements.txt`
- `.github/workflows/daily_digest.yml`
- `.gitignore`
