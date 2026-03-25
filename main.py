from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import List, Optional
import os

app = FastAPI(title="JIH JobSpy Microservice")

SCRAPE_SECRET = os.environ.get("SCRAPE_SECRET")


class ScrapeRequest(BaseModel):
    keywords: List[str]
    location: str = "Ottawa, Ontario, Canada"
    hours_old: int = 168  # 7 days
    results_per_keyword: int = 15
    sites: List[str] = ["linkedin", "indeed", "google", "zip_recruiter"]
    country: str = "Canada"


@app.post("/scrape")
async def scrape_jobs(request: ScrapeRequest, x_secret: str = Header(None)):
    if SCRAPE_SECRET and x_secret != SCRAPE_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    from jobspy import scrape_jobs
    import pandas as pd

    all_jobs = []
    errors = []

    for keyword in request.keywords:
        try:
            jobs = scrape_jobs(
                site_name=request.sites,
                search_term=keyword,
                location=request.location,
                results_wanted=request.results_per_keyword,
                hours_old=request.hours_old,
                country_indeed=request.country,
                linkedin_fetch_description=True,
                description_format="markdown",
            )

            if jobs is not None and len(jobs) > 0:
                # Replace NaN/None with None for JSON serialization
                jobs_list = jobs.where(pd.notnull(jobs), None).to_dict(orient="records")
                for job in jobs_list:
                    job["search_keyword"] = keyword
                    # Ensure job_url exists (used for dedup)
                    if not job.get("job_url"):
                        job["job_url"] = job.get("id", "")
                all_jobs.extend(jobs_list)

        except Exception as e:
            errors.append({"keyword": keyword, "error": str(e)})

    # Deduplicate by job_url at microservice level
    seen_urls = set()
    unique_jobs = []
    for job in all_jobs:
        url = str(job.get("job_url") or job.get("id") or "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_jobs.append(job)
        elif not url:
            unique_jobs.append(job)

    return {
        "jobs": unique_jobs,
        "total": len(unique_jobs),
        "errors": errors,
        "keywords_searched": request.keywords,
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "jih-jobspy-microservice"}
