# Daily Job Digest (Cloud Runner)

This repo runs a daily job search and emails a digest at **08:40 Europe/London**.

## What it does

- Searches for product roles in KYC/AML/onboarding domains.
- Includes roles posted in the last **72 hours**.
- Suppresses repeats using a cache (`sent_links.json`).
- Sends one email per day at 08:40 (daily schedule).

## Setup (GitHub Actions)

1. Create a new GitHub repo and push this folder.
2. Go to **Settings → Secrets and variables → Actions → New repository secret** and add:
   - `SMTP_HOST` (e.g., `smtp.gmail.com`)
   - `SMTP_PORT` (e.g., `587`)
   - `SMTP_USER` (your Gmail)
   - `SMTP_PASS` (Gmail App Password)
   - `FROM_EMAIL` (your Gmail)
   - `TO_EMAIL` (destination email)
3. Go to **Actions** and enable workflows.

## Schedule

- Workflow runs every 15 minutes, but only sends at **08:40 Europe/London**.
- The cache persists `sent_links.json` so repeats are not emailed.

## Files

- `daily_job_search.py`
- `requirements.txt`
- `.github/workflows/daily_digest.yml`
- `.gitignore`
