from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import List, Optional
import os
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="JIH JobSpy Microservice")
SCRAPE_SECRET = os.environ.get("SCRAPE_SECRET")
executor = ThreadPoolExecutor(max_workers=4)


class ScrapeRequest(BaseModel):
    keywords: List[str]
    location: str = "Ottawa, Ontario, Canada"
    hours_old: int = 168  # 7 days
    results_per_keyword: int = 5  # reduced to avoid timeouts
    sites: List[str] = ["linkedin", "indeed"]
    country: str = "Canada"


@app.get("/ping")
async def ping():
    """Instant health check — no scraping."""
    return {"status": "ok"}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "jih-jobspy-microservice"}


def _scrape_keyword(keyword: str, sites: List[str], location: str,
                    results_per_keyword: int, hours_old: int, country: str) -> List[dict]:
    """Synchronous scrape for one keyword — runs in thread executor."""
    from jobspy import scrape_jobs
    import pandas as pd

    logger.info(f"Scraping keyword: '{keyword}' on {sites} in {location}")
    jobs = scrape_jobs(
        site_name=sites,
        search_term=keyword,
        location=location,
        results_wanted=results_per_keyword,
        hours_old=hours_old,
        country_indeed=country,
        linkedin_fetch_description=False,  # skip full desc fetch to reduce timeout risk
        description_format="markdown",
    )

    if jobs is None or len(jobs) == 0:
        logger.info(f"Keyword '{keyword}': 0 results")
        return []

    jobs_list = jobs.where(pd.notnull(jobs), None).to_dict(orient="records")
    for job in jobs_list:
        job["search_keyword"] = keyword
        if not job.get("job_url"):
            job["job_url"] = job.get("id", "")

    logger.info(f"Keyword '{keyword}': {len(jobs_list)} results")
    return jobs_list


@app.post("/scrape")
async def scrape_jobs_endpoint(request: ScrapeRequest, x_secret: str = Header(None)):
    if SCRAPE_SECRET and x_secret != SCRAPE_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    all_jobs = []
    errors = []
    loop = asyncio.get_event_loop()

    for keyword in request.keywords:
        try:
            logger.info(f"Starting scrape for keyword: '{keyword}'")
            jobs = await asyncio.wait_for(
                loop.run_in_executor(
                    executor,
                    _scrape_keyword,
                    keyword,
                    request.sites,
                    request.location,
                    request.results_per_keyword,
                    request.hours_old,
                    request.country,
                ),
                timeout=60.0,
            )
            all_jobs.extend(jobs)
        except asyncio.TimeoutError:
            logger.warning(f"Keyword '{keyword}' timed out after 60s")
            errors.append({"keyword": keyword, "error": "Timeout after 60s"})
        except Exception as e:
            logger.error(f"Keyword '{keyword}' failed: {e}")
            errors.append({"keyword": keyword, "error": str(e)})

    # Deduplicate by job_url
    seen_urls: set = set()
    unique_jobs = []
    for job in all_jobs:
        url = str(job.get("job_url") or job.get("id") or "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_jobs.append(job)
        elif not url:
            unique_jobs.append(job)

    logger.info(f"Scrape complete: {len(unique_jobs)} unique jobs, {len(errors)} errors")
    return {
        "jobs": unique_jobs,
        "total": len(unique_jobs),
        "errors": errors,
        "keywords_searched": request.keywords,
    }
